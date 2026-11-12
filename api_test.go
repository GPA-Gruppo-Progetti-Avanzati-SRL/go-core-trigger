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

	if resp = api.Post(BasePath+"/nessuno", auth, map[string]any{}); resp.Code != http.StatusNotFound {
		t.Fatalf("task sconosciuto: %d", resp.Code)
	}
	if resp = api.Get(BasePath, auth); resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"kind":"s3"`) {
		t.Fatalf("elenco: %d %s", resp.Code, resp.Body.String())
	}
}
