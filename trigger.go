// Package trigger è l'attivazione dei task di go-core-batch dall'esterno: un'API, che un Job o
// CronJob Kubernetes chiama con esegui.sh (l'immagine in container/), per far partire un task e
// seguirlo fino all'esito.
//
// Il trigger di un task è il suo FEED, cioè ciò che lo mette in coda:
//
//   - task con un blocco `s3:` → il feed S3 della libreria (s3.Feeder): un work item per ogni file
//     del prefisso. La run "a finestra" segue i file e riesegue il feed finché il bucket non è vuoto;
//   - task senza `s3:` → un work item sul task (objectId = nome del task, payload = params): una sola
//     esecuzione attiva per task, e una seconda richiesta si aggancia a quella in corso.
//
// Ad ESEGUIRE i work item resta il job del task (SingleTask o DistribuiteTask che gira ogni tot): il
// trigger accoda e osserva, non esegue nulla. Lo stato della run si ricalcola dai work item a ogni
// lettura, quindi non c'è una tabella delle run ed è identico su Mongo, PostgreSQL e Oracle.
//
// Wiring:
//
//	trigger.Module(&svc.Trigger, svc.Batch.TasksConfig)
//	coreapi.Module(&svc.Api, coreapi.WithRoutes(trigger.Register), …)
//	batch.Module(&svc.Batch, …, batch.WithS3(s3feed.Module)) // se ci sono task con s3:
package trigger

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
	"uuid"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"github.com/rs/zerolog/log"
)

// OutcomeReader è il seam facoltativo con cui l'applicativo dà l'esito di un work item concluso,
// quando lo stato del work item non basta: DONE dice "il runner ha ritornato nil", e un'app che
// distingue NOTHINGTODO o SYSTEMBUSY lo scrive in un registro suo. Se l'app ne fornisce uno a fx, il
// trigger lo consulta per le run di un item; ok = false (nessun esito registrato) vale SUCCESS per un
// item DONE. Un item FAILED è sempre ERROR, qualunque cosa dica il registro.
type OutcomeReader interface {
	Outcome(ctx context.Context, item *store.WorkItem) (result, message string, counters map[string]int64, ok bool)
}

// workItems è la parte di store.IWorkItemStore che serve al trigger.
type workItems interface {
	InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.Error)
	GetById(ctx context.Context, id string) (*store.WorkItem, *core.Error)
	List(ctx context.Context, taskName, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.Error)
}

// Trigger avvia e osserva le run. Si inietta come ogni altra dipendenza (Module lo fornisce): l'API
// di Register ne è il client, ma un'app può chiamarlo anche da sé.
type Trigger struct {
	cfg     *Config
	allowed map[string]string // task attivabili: chiave minuscola -> nome come scritto in config
	sources map[string]bool   // task con s3:, chiave minuscola
	items   workItems
	feeder  s3.Feeder
	outcome OutcomeReader
	now     func() time.Time
	// Intervallo fra due valutazioni nel long-poll. Una run S3 riesegue il feed (un elenco del
	// bucket) a ogni valutazione: per questo il suo passo è più lungo.
	pollItem, pollS3 time.Duration
}

func newTrigger(cfg *Config, tasks []task.Config, items workItems, feeder s3.Feeder, outcome OutcomeReader) (*Trigger, error) {
	declared := make(map[string]string, len(tasks))
	for _, tc := range tasks {
		declared[strings.ToLower(tc.Name)] = tc.Name
	}
	t := &Trigger{
		cfg: cfg, allowed: map[string]string{}, sources: map[string]bool{},
		items: items, feeder: feeder, outcome: outcome, now: time.Now,
		pollItem: time.Second, pollS3: 5 * time.Second,
	}
	if len(cfg.Tasks) == 0 {
		t.allowed = declared
	}
	for _, name := range cfg.Tasks {
		n, ok := declared[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("trigger: il task %q di services.trigger.tasks non è dichiarato in tasks:", name)
		}
		t.allowed[strings.ToLower(name)] = n
	}
	for _, tc := range tasks {
		if tc.S3 == nil {
			continue
		}
		if feeder == nil {
			return nil, fmt.Errorf("trigger: il task %q ha un blocco s3: ma il feed S3 non è wirato: passare s3feed.Module a batch.WithS3", tc.Name)
		}
		t.sources[strings.ToLower(tc.Name)] = true
	}
	return t, nil
}

// resolve ritorna il nome del task come scritto in config, se è attivabile.
func (t *Trigger) resolve(name string) (string, *core.Error) {
	if n, ok := t.allowed[strings.ToLower(name)]; ok {
		return n, nil
	}
	if len(t.cfg.Tasks) > 0 {
		return "", forbidden(name)
	}
	return "", notFound(CodeUnknownTask, "task sconosciuto: "+name)
}

func (t *Trigger) kind(taskName string) string {
	if t.sources[strings.ToLower(taskName)] {
		return KindS3
	}
	return KindItem
}

// Tasks elenca i task attivabili, ordinati.
func (t *Trigger) Tasks(_ context.Context, authorization string) ([]TaskInfo, *core.Error) {
	if appErr := authenticate(t.cfg, authorization); appErr != nil {
		return nil, appErr
	}
	out := make([]TaskInfo, 0, len(t.allowed))
	for _, n := range t.allowed {
		out = append(out, TaskInfo{Task: n, Kind: t.kind(n)})
	}
	slices.SortFunc(out, func(a, b TaskInfo) int { return strings.Compare(a.Task, b.Task) })
	return out, nil
}

// Start avvia una run del task. Per un task senza s3: accoda un work item (params nel payload) o si
// aggancia a quello già attivo; per un task con s3: esegue il feed, e col bucket vuoto la run torna
// già conclusa NOTHINGTODO.
func (t *Trigger) Start(ctx context.Context, authorization, taskName string, params map[string]any) (*Run, *core.Error) {
	if appErr := authenticate(t.cfg, authorization); appErr != nil {
		return nil, appErr
	}
	name, appErr := t.resolve(taskName)
	if appErr != nil {
		return nil, appErr
	}
	if t.kind(name) == KindS3 {
		return t.startS3(ctx, name)
	}
	return t.startItem(ctx, name, params)
}

// Get legge lo stato di una run, attendendo al più wait (limitato da max-wait) che si concluda. Ogni
// valutazione di una run S3 la fa anche avanzare: riaccoda i file arrivati nel frattempo.
func (t *Trigger) Get(ctx context.Context, authorization, runID string, wait time.Duration) (*Run, *core.Error) {
	if appErr := authenticate(t.cfg, authorization); appErr != nil {
		return nil, appErr
	}
	ref, ok := parseRunID(runID)
	if !ok {
		return nil, notFound(CodeUnknownRun, "runId non valido: "+runID)
	}
	wait = min(max(wait, 0), t.cfg.maxWait())
	deadline := t.now().Add(wait)
	poll := t.pollItem
	if ref.kind == KindS3 {
		poll = t.pollS3
	}
	for {
		run, appErr := t.evaluate(ctx, ref)
		if appErr != nil {
			return nil, appErr
		}
		if run.State == StateDone || !t.now().Add(poll).Before(deadline) {
			return run, nil
		}
		select {
		case <-time.After(poll):
		case <-ctx.Done():
			return run, nil
		}
	}
}

func (t *Trigger) evaluate(ctx context.Context, ref runRef) (*Run, *core.Error) {
	if ref.kind == KindS3 {
		name, appErr := t.resolve(ref.task)
		if appErr != nil {
			return nil, appErr
		}
		return t.settleS3(ctx, name, ref.since, false, nil), nil
	}
	item, appErr := t.items.GetById(ctx, ref.itemID)
	if appErr != nil {
		if appErr.StatusCode == 404 {
			return nil, notFound(CodeUnknownRun, "run non trovata: "+itemRunID(ref.itemID))
		}
		return nil, appErr
	}
	if _, appErr := t.resolve(item.TaskName); appErr != nil {
		return nil, appErr
	}
	return t.itemRun(ctx, item, false), nil
}

// ─── run di un item ────────────────────────────────────────────────────────────────────────────

func (t *Trigger) startItem(ctx context.Context, name string, params map[string]any) (*Run, *core.Error) {
	// Due tentativi: fra un InsertIfNotActive che non inserisce e la ricerca dell'item attivo,
	// quell'esecuzione può essere finita. Al secondo tentativo l'inserimento riesce.
	for range 2 {
		now := t.now()
		item := &store.WorkItem{
			Id:         uuid.NewV7().String(),
			TaskName:   name,
			ObjectId:   name, // con TaskName è la chiave di deduplica: una sola esecuzione attiva
			Status:     store.StatusPending,
			CreateTime: now,
			NextRunAt:  &now,
		}
		if len(params) > 0 {
			item.Payload = params
		}
		inserted, appErr := t.items.InsertIfNotActive(ctx, []*store.WorkItem{item})
		if appErr != nil {
			return nil, appErr
		}
		if inserted == 1 {
			return t.itemRun(ctx, item, false), nil
		}
		active, appErr := t.findActive(ctx, name, name)
		if appErr != nil {
			return nil, appErr
		}
		if active != nil {
			return t.itemRun(ctx, active, true), nil
		}
	}
	return nil, errs.Tech(CodeStart).WithMessage("esecuzione attiva non trovata dopo la deduplica: task " + name)
}

// findActive cerca l'esecuzione attiva del task: prima quella in corso, poi quella in coda.
func (t *Trigger) findActive(ctx context.Context, taskName, objectID string) (*store.WorkItem, *core.Error) {
	for _, status := range []string{store.StatusInProgress, store.StatusPending} {
		items, appErr := t.items.List(ctx, taskName, status, page.InitPaging(nil, 50, 1, 0), nil)
		if appErr != nil {
			return nil, appErr
		}
		for _, it := range items {
			if it.ObjectId == objectID {
				return it, nil
			}
		}
	}
	return nil, nil
}

func isTerminal(status string) bool {
	return status == store.StatusDone || status == store.StatusFailed
}

// itemRun traduce lo stato del work item (e l'esito dell'OutcomeReader, se c'è) in una Run.
func (t *Trigger) itemRun(ctx context.Context, item *store.WorkItem, attached bool) *Run {
	run := &Run{
		RunId: itemRunID(item.Id), Task: item.TaskName, Kind: KindItem,
		State: StateRunning, Retry: item.Retry, Attached: attached,
	}
	if !isTerminal(item.Status) {
		return run
	}
	result, message, counters, ok := "", "", map[string]int64(nil), false
	if t.outcome != nil {
		result, message, counters, ok = t.outcome.Outcome(ctx, item)
	}
	if item.Status == store.StatusFailed {
		if message == "" {
			message = item.Error
		}
		return run.done(ResultError, message, counters)
	}
	if !ok {
		return run.done(ResultSuccess, message, counters)
	}
	return run.done(result, message, counters)
}

// ─── run S3 ────────────────────────────────────────────────────────────────────────────────────

func (t *Trigger) startS3(ctx context.Context, name string) (*Run, *core.Error) {
	// L'istante è troncato al millisecondo: è la risoluzione del runId, e gli item creati dal feed
	// che segue hanno CreateTime successivi, quindi rientrano tutti nella run.
	since := t.now().Truncate(time.Millisecond)
	active, appErr := t.firstActive(ctx, name)
	if appErr != nil {
		return nil, appErr
	}
	res, appErr := t.feeder.Feed(ctx, name)
	if appErr != nil {
		return nil, appErr
	}
	return t.settleS3(ctx, name, since, active != nil, &res), nil
}

// settleS3 ricalcola la run S3 dai work item del task e la fa avanzare. In ordine:
//
//  1. scaduta (run-ttl) → ERROR;
//  2. un item in coda o in lavorazione → RUNNING;
//  3. un item FAILED creato dall'inizio della run → ERROR, senza riaccodare: il file è ancora nel
//     prefisso, e il feed (un item FAILED non è attivo) lo riaccoderebbe facendolo rielaborare senza
//     fine — lo riprende la run successiva;
//  4. altrimenti il feed: file nuovi → RUNNING, bucket vuoto → SUCCESS (file elaborati) o NOTHINGTODO.
//
// fed è l'esito di un feed appena eseguito (l'avvio), per non elencare il bucket due volte. Un errore
// di lettura o di feed non chiude la run: si riprova alla valutazione successiva, e il TTL ne
// garantisce la fine.
func (t *Trigger) settleS3(ctx context.Context, name string, since time.Time, attached bool, fed *s3.FeedResult) *Run {
	run := &Run{RunId: s3RunID(name, since), Task: name, Kind: KindS3, State: StateRunning, Attached: attached}
	retry := func(what string, err error) *Run {
		log.Warn().Err(err).Str("task", name).Str("run", run.RunId).Msg("trigger: " + what + " non riuscito, si riprova")
		run.Message = what + " non riuscito, si riprova: " + err.Error()
		return run
	}

	if ttl := t.cfg.runTTL(); t.now().Sub(since) > ttl {
		return run.done(ResultError, fmt.Sprintf("run scaduta dopo %s", ttl), t.filesDone(ctx, name, since))
	}
	active, appErr := t.firstActive(ctx, name)
	if appErr != nil {
		return retry("lettura dei work item", appErr)
	}
	if active != nil {
		return run
	}
	failed, appErr := t.firstSince(ctx, name, store.StatusFailed, since)
	if appErr != nil {
		return retry("lettura dei work item", appErr)
	}
	if failed != nil {
		return run.done(ResultError, fmt.Sprintf("file %s non elaborato: %s", failed.ObjectId, failed.Error), t.filesDone(ctx, name, since))
	}
	if fed == nil {
		res, appErr := t.feeder.Feed(ctx, name)
		if appErr != nil {
			return retry("feed S3", appErr)
		}
		fed = &res
	}
	if fed.Outcome() != s3.NothingToDo {
		return run
	}
	files := t.filesDone(ctx, name, since)
	if files["files"] == 0 {
		return run.done(ResultNothingToDo, "nessun file da elaborare", files)
	}
	return run.done(ResultSuccess, fmt.Sprintf("%d file elaborati", files["files"]), files)
}

// firstActive ritorna un item del task in coda o in lavorazione, nil se non ce ne sono.
func (t *Trigger) firstActive(ctx context.Context, name string) (*store.WorkItem, *core.Error) {
	for _, status := range []string{store.StatusPending, store.StatusInProgress} {
		it, appErr := t.firstSince(ctx, name, status, time.Time{})
		if appErr != nil || it != nil {
			return it, appErr
		}
	}
	return nil, nil
}

// firstSince ritorna il più recente item del task nello stato dato creato da since, nil se non c'è.
// List ordina per creazione decrescente, quindi basta guardare il primo.
func (t *Trigger) firstSince(ctx context.Context, name, status string, since time.Time) (*store.WorkItem, *core.Error) {
	items, appErr := t.items.List(ctx, name, status, page.InitPaging(nil, 1, 1, 0), nil)
	if appErr != nil {
		return nil, appErr
	}
	if len(items) > 0 && !items[0].CreateTime.Before(since) {
		return items[0], nil
	}
	return nil, nil
}

// filesDone conta gli item DONE creati da since (al più 5000). È un dato informativo: un errore di
// lettura lo tronca senza far fallire la chiusura della run.
func (t *Trigger) filesDone(ctx context.Context, name string, since time.Time) map[string]int64 {
	var n int64
	for p := 1; p <= 50; p++ {
		items, appErr := t.items.List(ctx, name, store.StatusDone, page.InitPaging(nil, 100, p, 0), nil)
		if appErr != nil || len(items) == 0 {
			break
		}
		for _, it := range items {
			if it.CreateTime.Before(since) {
				return map[string]int64{"files": n}
			}
			n++
		}
	}
	return map[string]int64{"files": n}
}
