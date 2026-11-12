package trigger

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

func TestAuthenticate(t *testing.T) {
	cfg := &Config{Token: token}
	casi := map[string]string{
		"":                      CodeTokenMissing,
		"Basic abc":             CodeTokenMissing,
		"Bearer ":               CodeTokenMissing,
		"Bearer sbagliato":      CodeTokenInvalid,
		"Bearer " + token + "x": CodeTokenInvalid,
	}
	for header, code := range casi {
		if appErr := authenticate(cfg, false, header); appErr == nil || appErr.Code != code || appErr.StatusCode != 401 {
			t.Errorf("%q: atteso 401 %s, ottenuto %v", header, code, appErr)
		}
	}
	if appErr := authenticate(cfg, false, "  "+bearer+" "); appErr != nil {
		t.Errorf("token valido rifiutato: %v", appErr)
	}
	if appErr := authenticate(&Config{}, true, ""); appErr != nil {
		t.Errorf("autenticazione spenta (develop-mode): %v", appErr)
	}
	// Senza un token utilizzabile e non aperto (Trigger usato senza Register): tutto rifiutato, anche
	// chi presenta il segnaposto letterale come token.
	for _, cfg := range []*Config{{}, {Token: "${TRIGGER_TOKEN}"}} {
		if appErr := authenticate(cfg, false, "Bearer ${TRIGGER_TOKEN}"); appErr == nil || appErr.StatusCode != 401 {
			t.Errorf("token %q non utilizzabile: atteso 401, ottenuto %v", cfg.Token, appErr)
		}
	}
}

func TestConfig_TokenProblem(t *testing.T) {
	if (&Config{Token: token}).tokenProblem() != "" {
		t.Error("token valido segnalato")
	}
	if p := (&Config{}).tokenProblem(); p == "" {
		t.Error("token assente: atteso un problema")
	}
	if p := (&Config{Token: "${TRIGGER_TOKEN}"}).tokenProblem(); !strings.Contains(p, "non risolto") {
		t.Errorf("segnaposto non risolto: %q", p)
	}
	if err := (&Config{MaxWait: -1}).Validate(); err == nil {
		t.Error("max-wait negativo: atteso errore")
	}
}

func TestRunID(t *testing.T) {
	ref, ok := parseRunID(itemRunID("abc-123"))
	if !ok || ref.kind != KindItem || ref.itemID != "abc-123" {
		t.Fatalf("item: %+v %v", ref, ok)
	}
	since := t0.Add(1234 * time.Millisecond)
	ref, ok = parseRunID(s3RunID("flusso:a", since))
	if !ok || ref.kind != KindS3 || ref.task != "flusso:a" || !ref.since.Equal(since) {
		t.Fatalf("s3: %+v %v", ref, ok)
	}
	for _, bad := range []string{"", "item:", "s3:", "s3:import", "s3:import:x", "s3::12", "altro:1"} {
		if _, ok := parseRunID(bad); ok {
			t.Errorf("%q: atteso non valido", bad)
		}
	}
}

func TestNewTrigger(t *testing.T) {
	if _, err := newTrigger(&Config{Token: token}, testTasks(), &storeFake{}, nil, nil, nil); err == nil ||
		!strings.Contains(err.Error(), "batch.WithS3") {
		t.Fatalf("task con s3: senza Feeder: atteso un errore che nomini batch.WithS3, ottenuto %v", err)
	}
	if _, err := newTrigger(&Config{Token: token, Tasks: []string{"nessuno"}}, testTasks(), &storeFake{}, &feederFake{}, nil, nil); err == nil {
		t.Fatal("task ammesso non dichiarato: atteso errore")
	}
}

func TestStart_TaskSconosciutoONonAmmesso(t *testing.T) {
	tr := newTestTrigger(&storeFake{}, &feederFake{}, nil)
	if _, appErr := tr.Start(context.Background(), bearer, "nessuno", "", nil); appErr == nil || appErr.StatusCode != 404 || appErr.Code != CodeUnknownTask {
		t.Fatalf("task sconosciuto: %v", appErr)
	}
	tr.cfg.Tasks = []string{"import"}
	tr.allowed = map[string]string{"import": "import"}
	if _, appErr := tr.Start(context.Background(), bearer, "selftest", "", nil); appErr == nil || appErr.StatusCode != 403 {
		t.Fatalf("task non ammesso: atteso 403, ottenuto %v", appErr)
	}
	if _, appErr := tr.Start(context.Background(), "Bearer no", "import", "", nil); appErr == nil || appErr.StatusCode != 401 {
		t.Fatalf("token errato: atteso 401, ottenuto %v", appErr)
	}
}

// ─── run di un item ───

func TestItemRun(t *testing.T) {
	st := &storeFake{}
	tr := newTestTrigger(st, &feederFake{}, nil)
	ctx := context.Background()

	run, appErr := tr.Start(ctx, bearer, "SELFTEST", "", map[string]any{"asOf": "2026-11-12"})
	if appErr != nil || run.State != StateRunning || run.Attached || run.Kind != KindItem {
		t.Fatalf("avvio: %+v %v", run, appErr)
	}
	item := st.items[0]
	if item.TaskName != "selftest" || item.ObjectId != "selftest" || item.Payload.(map[string]any)["asOf"] != "2026-11-12" {
		t.Fatalf("item accodato: %+v", item)
	}

	// Una seconda richiesta durante l'esecuzione si aggancia, senza accodarne un'altra.
	again, appErr := tr.Start(ctx, bearer, "selftest", "", nil)
	if appErr != nil || !again.Attached || again.RunId != run.RunId || len(st.items) != 1 {
		t.Fatalf("aggancio: %+v %v (%d item)", again, appErr, len(st.items))
	}

	item.Status = store.StatusDone
	got, appErr := tr.Get(ctx, bearer, run.RunId, 0)
	if appErr != nil || got.State != StateDone || got.Result != ResultSuccess || *got.ExitCode != 0 {
		t.Fatalf("DONE: %+v %v", got, appErr)
	}

	item.Status, item.Error = store.StatusFailed, "esploso"
	got, _ = tr.Get(ctx, bearer, run.RunId, 0)
	if got.Result != ResultError || *got.ExitCode != 1 || got.Message != "esploso" {
		t.Fatalf("FAILED: %+v", got)
	}

	if _, appErr := tr.Get(ctx, bearer, itemRunID("inesistente"), 0); appErr == nil || appErr.Code != CodeUnknownRun {
		t.Fatalf("run inesistente: %v", appErr)
	}
	if _, appErr := tr.Get(ctx, bearer, "spazzatura", 0); appErr == nil || appErr.StatusCode != 404 {
		t.Fatalf("runId malformato: %v", appErr)
	}
}

func TestItemRun_OutcomeReader(t *testing.T) {
	st := &storeFake{}
	tr := newTestTrigger(st, &feederFake{}, outcomeFake{result: ResultNothingToDo, ok: true})
	run, _ := tr.Start(context.Background(), bearer, "selftest", "", nil)
	st.items[0].Status = store.StatusDone
	got, _ := tr.Get(context.Background(), bearer, run.RunId, 0)
	if got.Result != ResultNothingToDo || *got.ExitCode != 4 || got.Counters["righe"] != 3 {
		t.Fatalf("esito dal registro: %+v", got)
	}
	// Un item FAILED è ERROR qualunque cosa dica il registro.
	st.items[0].Status = store.StatusFailed
	if got, _ = tr.Get(context.Background(), bearer, run.RunId, 0); got.Result != ResultError {
		t.Fatalf("FAILED col registro: %+v", got)
	}
}

func TestGet_LongPollSiFermaAllaConclusione(t *testing.T) {
	st := &storeFake{}
	tr := newTestTrigger(st, &feederFake{}, nil)
	tr.now = time.Now
	tr.pollItem = 10 * time.Millisecond
	run, _ := tr.Start(context.Background(), bearer, "selftest", "", nil)
	go func() {
		time.Sleep(30 * time.Millisecond)
		st.setStatus(0, store.StatusDone)
	}()
	start := time.Now()
	got, _ := tr.Get(context.Background(), bearer, run.RunId, 5*time.Second)
	if got.State != StateDone || time.Since(start) > 2*time.Second {
		t.Fatalf("long-poll: %+v dopo %s", got, time.Since(start))
	}
}

// ─── run S3 ───

func TestS3Run_BucketVuotoChiudeSubito(t *testing.T) {
	feeder := &feederFake{}
	tr := newTestTrigger(&storeFake{}, feeder, nil)
	run, appErr := tr.Start(context.Background(), bearer, "import", "", nil)
	if appErr != nil || run.State != StateDone || run.Result != ResultNothingToDo || *run.ExitCode != 4 || feeder.calls != 1 {
		t.Fatalf("bucket vuoto: %+v %v (feed %d)", run, appErr, feeder.calls)
	}
	// La lettura successiva dà lo stesso esito: lo script la fa sempre.
	got, _ := tr.Get(context.Background(), bearer, run.RunId, 0)
	if got.Result != ResultNothingToDo {
		t.Fatalf("rilettura: %+v", got)
	}
}

func TestS3Run_CicloCompleto(t *testing.T) {
	st := &storeFake{}
	feeder := &feederFake{files: []string{"inbox/1.dat", "inbox/2.dat"}}
	tr := newTestTrigger(st, feeder, nil)
	ctx := context.Background()

	run, appErr := tr.Start(ctx, bearer, "import", "", nil)
	if appErr != nil || run.State != StateRunning || len(st.items) != 2 || run.Kind != KindS3 {
		t.Fatalf("avvio: %+v %v (%d item)", run, appErr, len(st.items))
	}

	// File in lavorazione: RUNNING, e nessun feed finché c'è qualcosa in coda.
	calls := feeder.calls
	if got, _ := tr.Get(ctx, bearer, run.RunId, 0); got.State != StateRunning || feeder.calls != calls {
		t.Fatalf("in lavorazione: %+v, feed %d→%d", got, calls, feeder.calls)
	}

	// Elaborati e spostati: il bucket è vuoto.
	for _, it := range st.items {
		it.Status = store.StatusDone
	}
	feeder.files = nil
	got, _ := tr.Get(ctx, bearer, run.RunId, 0)
	if got.State != StateDone || got.Result != ResultSuccess || got.Counters["files"] != 2 {
		t.Fatalf("fine: %+v", got)
	}
}

func TestS3Run_FileArrivatiNelFrattempoSiAccodano(t *testing.T) {
	st := &storeFake{}
	feeder := &feederFake{files: []string{"a"}}
	tr := newTestTrigger(st, feeder, nil)
	run, _ := tr.Start(context.Background(), bearer, "import", "", nil)
	st.items[0].Status = store.StatusDone
	feeder.files = []string{"b"} // "a" spostato, "b" arrivato
	if got, _ := tr.Get(context.Background(), bearer, run.RunId, 0); got.State != StateRunning || len(st.items) != 2 {
		t.Fatalf("file arrivato: %+v (%d item)", got, len(st.items))
	}
}

// Il file fallito è ancora nel prefisso: riaccodarlo lo farebbe rielaborare senza fine.
func TestS3Run_FileFallitoChiudeErroreSenzaRiaccodare(t *testing.T) {
	st := &storeFake{}
	feeder := &feederFake{files: []string{"inbox/A.txt"}}
	tr := newTestTrigger(st, feeder, nil)
	run, _ := tr.Start(context.Background(), bearer, "import", "", nil)
	st.items[0].Status = store.StatusFailed
	calls := feeder.calls
	got, _ := tr.Get(context.Background(), bearer, run.RunId, 0)
	if got.Result != ResultError || !strings.Contains(got.Message, "A.txt") || feeder.calls != calls {
		t.Fatalf("fallito: %+v, feed %d→%d", got, calls, feeder.calls)
	}
}

func TestS3Run_FallitoDiUnaRunPrecedenteSiIgnora(t *testing.T) {
	st := &storeFake{}
	st.add("import", "VECCHIO", store.StatusFailed, t0.Add(-time.Hour))
	tr := newTestTrigger(st, &feederFake{}, nil)
	if run, _ := tr.Start(context.Background(), bearer, "import", "", nil); run.Result != ResultNothingToDo {
		t.Fatalf("il FAILED è precedente alla run: %+v", run)
	}
}

func TestS3Run_AgganciataSeCeLavoroInCorso(t *testing.T) {
	st := &storeFake{}
	st.add("import", "in-corso", store.StatusInProgress, t0.Add(-time.Minute))
	tr := newTestTrigger(st, &feederFake{}, nil)
	if run, _ := tr.Start(context.Background(), bearer, "import", "", nil); !run.Attached || run.State != StateRunning {
		t.Fatalf("lavoro in corso: %+v", run)
	}
}

func TestS3Run_TTLEFeedFallito(t *testing.T) {
	st := &storeFake{}
	feeder := &feederFake{}
	tr := newTestTrigger(st, feeder, nil)
	ctx := context.Background()

	// Feed fallito all'avvio: errore al chiamante (lo script lo riporta come non avviata).
	feeder.err = core.TechnicalError().WithCode("BATCH-S3-LIST")
	if _, appErr := tr.Start(ctx, bearer, "import", "", nil); appErr == nil || appErr.Code != "BATCH-S3-LIST" {
		t.Fatalf("feed fallito all'avvio: %v", appErr)
	}
	// Feed fallito durante la run: resta RUNNING, si riprova.
	runID := s3RunID("import", t0.Add(-time.Minute))
	got, appErr := tr.Get(ctx, bearer, runID, 0)
	if appErr != nil || got.State != StateRunning || !strings.Contains(got.Message, "si riprova") {
		t.Fatalf("feed fallito in corsa: %+v %v", got, appErr)
	}
	// Oltre run-ttl: ERROR.
	old := s3RunID("import", t0.Add(-13*time.Hour))
	if got, _ = tr.Get(ctx, bearer, old, 0); got.Result != ResultError || !strings.Contains(got.Message, "scaduta") {
		t.Fatalf("scaduta: %+v", got)
	}
}

func TestTasks(t *testing.T) {
	tr := newTestTrigger(&storeFake{}, &feederFake{}, nil)
	got, appErr := tr.Tasks(context.Background(), bearer)
	if appErr != nil || len(got) != 2 || got[0] != (TaskInfo{Task: "import", Kind: KindS3}) || got[1] != (TaskInfo{Task: "selftest", Kind: KindItem}) {
		t.Fatalf("elenco: %+v %v", got, appErr)
	}
}

// preparerFake valida l'oggetto e costruisce il payload, come un'app che lavora un'entità per item.
type preparerFake struct{ calls int }

func (p *preparerFake) Prepare(_ context.Context, task, objectID string, params map[string]any) (any, *core.Error) {
	p.calls++
	if objectID == "" {
		return nil, core.BusinessError().WithCode("OBJ").WithMessage("oggetto obbligatorio per " + task)
	}
	if objectID == "IGNOTO" {
		return nil, core.NotFoundError()
	}
	return map[string]any{"id": objectID, "params": len(params)}, nil
}

// Una run su un oggetto: la deduplica è per (task, oggetto), il payload è del Preparer, e un errore
// del Preparer è la risposta senza accodare nulla.
func TestItemRun_Oggetto(t *testing.T) {
	st := &storeFake{}
	tr := newTestTrigger(st, &feederFake{}, nil)
	p := &preparerFake{}
	tr.prepare = p
	ctx := context.Background()

	a, appErr := tr.Start(ctx, bearer, "selftest", "A", map[string]any{"x": 1})
	if appErr != nil || a.Attached {
		t.Fatalf("avvio A: %+v %v", a, appErr)
	}
	if it := st.items[0]; it.ObjectId != "A" || it.Payload.(map[string]any)["id"] != "A" {
		t.Fatalf("item A: %+v", it)
	}
	b, appErr := tr.Start(ctx, bearer, "selftest", "B", nil)
	if appErr != nil || b.Attached || b.RunId == a.RunId || len(st.items) != 2 {
		t.Fatalf("B è un altro oggetto, va accodato: %+v %v (%d item)", b, appErr, len(st.items))
	}
	again, appErr := tr.Start(ctx, bearer, "selftest", "A", nil)
	if appErr != nil || !again.Attached || again.RunId != a.RunId || len(st.items) != 2 {
		t.Fatalf("A di nuovo si aggancia: %+v %v (%d item)", again, appErr, len(st.items))
	}

	if _, appErr := tr.Start(ctx, bearer, "selftest", "IGNOTO", nil); appErr == nil || appErr.StatusCode != 404 {
		t.Fatalf("oggetto sconosciuto: atteso il 404 del Preparer, ottenuto %v", appErr)
	}
	if _, appErr := tr.Start(ctx, bearer, "selftest", "", nil); appErr == nil || appErr.Code != "OBJ" {
		t.Fatalf("senza oggetto: atteso l'errore del Preparer, ottenuto %v", appErr)
	}
	if len(st.items) != 2 {
		t.Fatalf("un errore del Preparer non deve accodare: %d item", len(st.items))
	}

	// Un task con s3: non prende un oggetto, e il Preparer non è consultato.
	calls := p.calls
	if _, appErr := tr.Start(ctx, bearer, "import", "A", nil); appErr == nil || appErr.Code != CodeObjectOnS3 || appErr.StatusCode != 422 {
		t.Fatalf("s3 con oggetto: %v", appErr)
	}
	if p.calls != calls {
		t.Fatal("Preparer consultato per un task con s3:")
	}
}
