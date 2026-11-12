package trigger

import (
	"net/http"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// Ambit è l'ambito degli errori nati in questa libreria.
const Ambit = "go-core-trigger"

// Codici d'errore. Il catalogo completo è in ERRORI.md.
const (
	CodeTokenMissing = "TRIGGER-AUTH-001" // 401: Authorization assente o non Bearer
	CodeTokenInvalid = "TRIGGER-AUTH-002" // 401: token diverso da quello condiviso
	CodeTaskDenied   = "TRIGGER-AUTH-003" // 403: task non fra quelli ammessi (services.trigger.tasks)
	CodeUnknownTask  = "TRIGGER-TASK"     // 404: task non dichiarato in tasks:
	CodeUnknownRun   = "TRIGGER-RUN"      // 404: runId malformato o work item inesistente
	CodeStart        = "TRIGGER-START"    // 500: avvio della run fallito (store, feed S3)
)

var errs = core.AmbitErrors{Ambit: Ambit}

func withStatus(e *core.Error, status int) *core.Error {
	e.StatusCode = status
	return e
}

func unauthorized(code, message string) *core.Error {
	return withStatus(errs.Business(code).WithMessage(message), http.StatusUnauthorized)
}

func forbidden(task string) *core.Error {
	return withStatus(errs.Business(CodeTaskDenied).WithMessage("task non attivabile da trigger: "+task), http.StatusForbidden)
}

func notFound(code, message string) *core.Error {
	return withStatus(errs.Business(code).WithMessage(message), http.StatusNotFound)
}
