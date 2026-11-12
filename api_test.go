package trigger

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	coreapi "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-api"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/danielgtaylor/huma/v2/humatest"
)

// Le rotte vere, sul Router di go-core-api: è il contratto che esegui.sh legge.
func TestAPI(t *testing.T) {
	_, api := humatest.New(t)
	st := &storeFake{}
	tr := newTestTrigger(st, &feederFake{}, nil)
	Register(&coreapi.Router{Api: api}, tr)
	auth := "Authorization: " + bearer

	if resp := api.Post(BasePath+"/selftest", map[string]any{}); resp.Code != http.StatusUnauthorized {
		t.Fatalf("senza token: %d %s", resp.Code, resp.Body.String())
	}

	resp := api.Post(BasePath+"/selftest", auth, map[string]any{"params": map[string]any{"x": 1}})
	if resp.Code != http.StatusAccepted {
		t.Fatalf("avvio: %d %s", resp.Code, resp.Body.String())
	}
	var run Run
	if err := json.Unmarshal(resp.Body.Bytes(), &run); err != nil || !strings.HasPrefix(run.RunId, "item:") {
		t.Fatalf("risposta: %s (%v)", resp.Body.String(), err)
	}

	if resp = api.Post(BasePath+"/selftest", auth, map[string]any{}); resp.Code != http.StatusOK {
		t.Fatalf("aggancio: atteso 200, ottenuto %d", resp.Code)
	}

	st.items[0].Status = store.StatusDone
	resp = api.Get(BasePath+"/runs/"+run.RunId+"?wait=1", auth)
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"exitCode":0`) {
		t.Fatalf("stato: %d %s", resp.Code, resp.Body.String())
	}

	// Run S3 col bucket vuoto: 200 già conclusa NOTHINGTODO.
	resp = api.Post(BasePath+"/import", auth, map[string]any{})
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"result":"NOTHINGTODO"`) {
		t.Fatalf("s3 vuoto: %d %s", resp.Code, resp.Body.String())
	}

	// Run su un oggetto: un item per (task, oggetto).
	resp = api.Post(BasePath+"/selftest/ENT_1", auth, map[string]any{})
	if resp.Code != http.StatusAccepted {
		t.Fatalf("avvio su oggetto: %d %s", resp.Code, resp.Body.String())
	}
	if it := st.items[len(st.items)-1]; it.ObjectId != "ENT_1" || it.TaskName != "selftest" {
		t.Fatalf("item su oggetto: %+v", it)
	}
	if resp = api.Post(BasePath+"/import/ENT_1", auth, map[string]any{}); resp.Code != http.StatusUnprocessableEntity {
		t.Fatalf("s3 con oggetto: atteso 422, ottenuto %d", resp.Code)
	}

	if resp = api.Post(BasePath+"/nessuno", auth, map[string]any{}); resp.Code != http.StatusNotFound {
		t.Fatalf("task sconosciuto: %d", resp.Code)
	}
	if resp = api.Get(BasePath, auth); resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"kind":"s3"`) {
		t.Fatalf("elenco: %d %s", resp.Code, resp.Body.String())
	}
}

// Il token è obbligatorio fuori dal develop-mode (panic al wiring) e facoltativo dentro, dove
// l'autenticazione si spegne.
func TestRegister_TokenEDevelopMode(t *testing.T) {
	for _, tok := range []string{"", "${TRIGGER_TOKEN}"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("token %q fuori dal develop-mode: atteso panic", tok)
				}
			}()
			_, api := humatest.New(t)
			tr := newTestTrigger(&storeFake{}, &feederFake{}, nil)
			tr.cfg = &Config{Token: tok}
			Register(&coreapi.Router{Api: api}, tr)
		}()
	}

	_, api := humatest.New(t)
	tr := newTestTrigger(&storeFake{}, &feederFake{}, nil)
	tr.cfg = &Config{Token: "${TRIGGER_TOKEN}"}
	Register(&coreapi.Router{Api: api, DevelopMode: true}, tr)
	if resp := api.Post(BasePath+"/selftest", map[string]any{}); resp.Code != http.StatusAccepted {
		t.Fatalf("develop-mode senza token: attesa 202 senza Authorization, ottenuto %d %s", resp.Code, resp.Body.String())
	}

	// Col token, anche in develop-mode l'autenticazione resta.
	_, api = humatest.New(t)
	tr = newTestTrigger(&storeFake{}, &feederFake{}, nil)
	Register(&coreapi.Router{Api: api, DevelopMode: true}, tr)
	if resp := api.Post(BasePath+"/selftest", map[string]any{}); resp.Code != http.StatusUnauthorized {
		t.Fatalf("develop-mode col token: atteso 401 senza Authorization, ottenuto %d", resp.Code)
	}
}
