package trigger

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Stati di una run.
const (
	StateRunning = "RUNNING"
	StateDone    = "DONE"
)

// Esiti di una run conclusa, con il codice d'uscita che esegui.sh restituisce: gli stessi di
// invokeBatch.sh, così i job stream TWS che ne leggono il codice non cambiano.
const (
	ResultSuccess     = "SUCCESS"
	ResultError       = "ERROR"
	ResultNothingToDo = "NOTHINGTODO"
	ResultSystemBusy  = "SYSTEMBUSY"
)

// ExitCodes è il codice d'uscita di ogni esito.
var ExitCodes = map[string]int{
	ResultSuccess:     0,
	ResultError:       1,
	ResultNothingToDo: 4,
	ResultSystemBusy:  15,
}

// Kind di un task attivabile.
const (
	KindItem = "item" // il trigger accoda un work item
	KindS3   = "s3"   // il trigger esegue il feed S3 del task: un work item per file
)

// Run è lo stato di un'esecuzione avviata dal trigger, come lo legge esegui.sh. Result ed ExitCode
// ci sono solo a run conclusa.
type Run struct {
	RunId    string           `json:"runId"`
	Task     string           `json:"task"`
	Kind     string           `json:"kind" enum:"item,s3"`
	State    string           `json:"state" enum:"RUNNING,DONE"`
	Result   string           `json:"result,omitempty" enum:"SUCCESS,ERROR,NOTHINGTODO,SYSTEMBUSY"`
	ExitCode *int             `json:"exitCode,omitempty"`
	Message  string           `json:"message,omitempty"`
	Counters map[string]int64 `json:"counters,omitempty"`
	// Retry sono i ritentativi già consumati (solo run di un item).
	Retry int `json:"retry"`
	// Attached è true quando la richiesta ha trovato un'esecuzione già attiva e ci si è agganciata.
	Attached bool `json:"attached"`
}

// done chiude la run con l'esito.
func (r *Run) done(result, message string, counters map[string]int64) *Run {
	code, ok := ExitCodes[result]
	if !ok {
		result, code = ResultError, ExitCodes[ResultError]
	}
	r.State, r.Result, r.ExitCode, r.Message = StateDone, result, &code, message
	if counters != nil {
		r.Counters = counters
	}
	return r
}

// TaskInfo è un task attivabile, come lo elenca GET /api/v1/trigger.
type TaskInfo struct {
	Task string `json:"task"`
	Kind string `json:"kind" enum:"item,s3"`
}

// Il runId codifica tutto ciò che serve a ricalcolare lo stato della run dai work item: non c'è una
// tabella delle run.
//
//	item:<workItemId>              la run di un item: il suo stato è quello del work item
//	s3:<task>:<istante in ms>      la run S3: gli item del task creati da quell'istante
const (
	prefixItem = "item:"
	prefixS3   = "s3:"
)

func itemRunID(workItemID string) string { return prefixItem + workItemID }

func s3RunID(task string, since time.Time) string {
	return fmt.Sprintf("%s%s:%d", prefixS3, task, since.UnixMilli())
}

// runRef è un runId decodificato.
type runRef struct {
	kind   string
	itemID string    // KindItem
	task   string    // KindS3
	since  time.Time // KindS3
}

func parseRunID(id string) (runRef, bool) {
	if rest, ok := strings.CutPrefix(id, prefixItem); ok && rest != "" {
		return runRef{kind: KindItem, itemID: rest}, true
	}
	if rest, ok := strings.CutPrefix(id, prefixS3); ok {
		i := strings.LastIndexByte(rest, ':')
		if i <= 0 {
			return runRef{}, false
		}
		ms, err := strconv.ParseInt(rest[i+1:], 10, 64)
		if err != nil || ms <= 0 {
			return runRef{}, false
		}
		return runRef{kind: KindS3, task: rest[:i], since: time.UnixMilli(ms)}, true
	}
	return runRef{}, false
}
