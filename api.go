package trigger

import (
	"context"
	"net/http"
	"time"

	coreapi "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-api"
	"github.com/rs/zerolog/log"
)

// BasePath è il prefisso delle rotte del trigger.
const BasePath = "/api/v1/trigger"

const tag = "trigger"

// Register monta le rotte del trigger sul Router di go-core-api:
//
//	coreapi.Module(&svc.Api, coreapi.WithRoutes(trigger.Register))
//
// Le rotte si autenticano da sé col token condiviso. Il token è obbligatorio, salvo con
// `develop-mode: true` di go-core-api: lì, se manca (o è un segnaposto non risolto), l'autenticazione
// è spenta con un Warn, come le altre comodità di sviluppo del develop-mode. Fuori dal develop-mode un
// token mancante fa panic qui, cioè al wiring: l'applicativo non parte invece di esporre l'avvio dei
// task a chiunque lo raggiunga.
//
// Se l'app monta anche il middleware di go-core-auth, che nega le richieste senza ruoli, le path del
// trigger vanno escluse da lì.
func Register(r *coreapi.Router, t *Trigger) {
	if problem := t.cfg.tokenProblem(); problem != "" {
		if !r.DevelopMode {
			panic("trigger: " + problem + "; il token è facoltativo solo con api.develop-mode: true")
		}
		t.open = true
		log.Warn().Str("motivo", problem).Msg("trigger: develop-mode senza token, autenticazione spenta")
	}
	coreapi.RegisterWithBusiness(r, t, startOperation, start)
	coreapi.RegisterWithBusiness(r, t, startObjectOperation, startObject)
	coreapi.RegisterWithBusiness(r, t, getRunOperation, getRun)
	coreapi.RegisterWithBusiness(r, t, listOperation, list)
}

// StartBody è il corpo facoltativo di POST /api/v1/trigger/{task}[/{objectId}].
type StartBody struct {
	// Params finisce nel payload del work item (solo task senza s3:), o al Preparer dell'app.
	Params map[string]any `json:"params,omitempty"`
}

var startOperation = coreapi.Operation{
	OperationID: "TriggerStart",
	Method:      http.MethodPost,
	Path:        BasePath + "/{task}",
	Summary:     "Avvia una run del task (202), o si aggancia a quella attiva (200)",
	Description: "Il trigger di un task è il suo feed: con un blocco s3: accoda un work item per file, " +
		"altrimenti un work item. Una run già conclusa (bucket vuoto: NOTHINGTODO) risponde 200.",
	Tags: []string{tag},
}

type startInput struct {
	Authorization string     `header:"Authorization" required:"false" doc:"Bearer <token condiviso>"`
	Task          string     `path:"task" doc:"nome del task (voce di tasks:)"`
	Body          *StartBody // puntatore: il body è facoltativo
}

type startOutput struct {
	Status int
	Body   *Run
}

func start(ctx context.Context, in *startInput, t *Trigger) (*startOutput, error) {
	return startRun(ctx, t, in.Authorization, in.Task, "", in.Body)
}

var startObjectOperation = coreapi.Operation{
	OperationID: "TriggerStartObject",
	Method:      http.MethodPost,
	Path:        BasePath + "/{task}/{objectId}",
	Summary:     "Avvia una run del task su un oggetto (202), o si aggancia a quella attiva (200)",
	Description: "Per i task che lavorano un oggetto per work item (un'entità, un id): accoda un work item " +
		"con quell'objectId, e una seconda richiesta sullo stesso oggetto si aggancia alla run attiva. " +
		"Non vale per i task con s3: (422 TRIGGER-OBJECT).",
	Tags: []string{tag},
}

type startObjectInput struct {
	Authorization string     `header:"Authorization" required:"false" doc:"Bearer <token condiviso>"`
	Task          string     `path:"task" doc:"nome del task (voce di tasks:)"`
	ObjectID      string     `path:"objectId" doc:"oggetto su cui eseguire il task: objectId del work item"`
	Body          *StartBody // puntatore: il body è facoltativo
}

func startObject(ctx context.Context, in *startObjectInput, t *Trigger) (*startOutput, error) {
	return startRun(ctx, t, in.Authorization, in.Task, in.ObjectID, in.Body)
}

func startRun(ctx context.Context, t *Trigger, authorization, task, objectID string, body *StartBody) (*startOutput, error) {
	var params map[string]any
	if body != nil {
		params = body.Params
	}
	run, appErr := t.Start(ctx, authorization, task, objectID, params)
	if appErr != nil {
		return nil, coreapi.ManageBusinessError(appErr)
	}
	status := http.StatusAccepted
	if run.Attached || run.State == StateDone {
		status = http.StatusOK
	}
	return &startOutput{Status: status, Body: run}, nil
}

var getRunOperation = coreapi.Operation{
	OperationID: "TriggerGetRun",
	Method:      http.MethodGet,
	Path:        BasePath + "/runs/{runId}",
	Summary:     "Stato di una run, con attesa fino a wait secondi che si concluda",
	Tags:        []string{tag},
}

type getRunInput struct {
	Authorization string `header:"Authorization" required:"false" doc:"Bearer <token condiviso>"`
	RunID         string `path:"runId"`
	Wait          int    `query:"wait" minimum:"0" doc:"secondi di attesa massima (limitati da max-wait)"`
}

type runOutput struct {
	Body *Run
}

func getRun(ctx context.Context, in *getRunInput, t *Trigger) (*runOutput, error) {
	run, appErr := t.Get(ctx, in.Authorization, in.RunID, time.Duration(in.Wait)*time.Second)
	if appErr != nil {
		return nil, coreapi.ManageBusinessError(appErr)
	}
	return &runOutput{Body: run}, nil
}

var listOperation = coreapi.Operation{
	OperationID: "TriggerList",
	Method:      http.MethodGet,
	Path:        BasePath,
	Summary:     "Elenco dei task attivabili",
	Tags:        []string{tag},
}

type listInput struct {
	Authorization string `header:"Authorization" required:"false" doc:"Bearer <token condiviso>"`
}

type listOutput struct {
	Body []TaskInfo
}

func list(ctx context.Context, in *listInput, t *Trigger) (*listOutput, error) {
	tasks, appErr := t.Tasks(ctx, in.Authorization)
	if appErr != nil {
		return nil, coreapi.ManageBusinessError(appErr)
	}
	return &listOutput{Body: tasks}, nil
}
