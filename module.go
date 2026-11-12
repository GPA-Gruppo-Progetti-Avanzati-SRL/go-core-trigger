package trigger

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// declaredTasks è la sezione tasks: del batch, supplita al modulo con un tipo suo: il batch supplisce
// la stessa []task.Config ai propri moduli, e un tipo distinto non può confondersi con quella.
type declaredTasks []task.Config

// Module wira il trigger: è l'unico entry-point. cfg è la sezione services.trigger, tasks la sezione
// tasks: del batch (svc.Batch.TasksConfig), da cui il trigger sa quali task esistono e quali hanno un
// blocco s3:. Fornisce a fx il *Trigger, che Register monta sul Router di go-core-api.
//
//	trigger.Module(&svc.Trigger, svc.Batch.TasksConfig)
//	coreapi.Module(&svc.Api, coreapi.WithRoutes(trigger.Register))
//
// Dipendenze dal grafo: store.IWorkItemStore (batch.WithStore); s3.Feeder se un task ha s3:
// (batch.WithS3(s3feed.Module)), altrimenti l'avvio fallisce dicendolo; OutcomeReader facoltativo.
//
// Una config non valida (token assente con l'autenticazione attiva) fa panic qui: un trigger senza
// token esporrebbe l'avvio dei task a chiunque raggiunga l'applicativo.
func Module(cfg *Config, tasks []task.Config, modes ...string) {
	if err := cfg.Validate(); err != nil {
		panic(err.Error())
	}
	core.Module("trigger", func() {
		core.Supply(cfg, modes...)
		core.Supply(declaredTasks(tasks), modes...)
		core.Provide(provideTrigger, modes...)
	})
}

type params struct {
	core.In
	Cfg     *Config
	Tasks   declaredTasks
	Items   store.IWorkItemStore
	Feeder  s3.Feeder     `optional:"true"`
	Outcome OutcomeReader `optional:"true"`
}

func provideTrigger(p params) (*Trigger, error) {
	return newTrigger(p.Cfg, p.Tasks, p.Items, p.Feeder, p.Outcome)
}
