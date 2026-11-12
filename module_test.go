package trigger

import (
	"strings"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

// Il costruttore che Module registra: con un task s3: serve il Feeder dal grafo, e senza l'avvio
// fallisce dicendo cosa wirare.
func TestProvideTrigger_Wiring(t *testing.T) {
	build := func(extra ...fx.Option) error {
		var tr *Trigger
		opts := append([]fx.Option{
			fx.NopLogger,
			fx.Supply(&Config{Token: token}),
			fx.Supply(declaredTasks(testTasks())),
			fx.Provide(func() store.IWorkItemStore { return nil }),
			fx.Provide(provideTrigger),
			fx.Populate(&tr),
		}, extra...)
		return fx.New(opts...).Err()
	}
	if err := build(); err == nil || !strings.Contains(err.Error(), "batch.WithS3") {
		t.Fatalf("senza Feeder: atteso un errore che nomini batch.WithS3, ottenuto %v", err)
	}
	if err := build(fx.Provide(func() s3.Feeder { return &feederFake{} })); err != nil {
		t.Fatalf("col Feeder: %v", err)
	}
}

func TestModule_ConfigNonValidaPanica(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("token assente: atteso panic")
		}
	}()
	Module(&Config{}, testTasks())
}
