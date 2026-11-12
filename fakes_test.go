package trigger

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// storeFake è uno store in memoria: deduplica su (task, objectId) attivi come uk_workitem_active e
// List restituisce dal più recente, come i backend della libreria.
type storeFake struct {
	items []*store.WorkItem
	err   *core.Error
}

func (s *storeFake) InsertIfNotActive(_ context.Context, items []*store.WorkItem) (int, *core.Error) {
	if s.err != nil {
		return 0, s.err
	}
	n := 0
	for _, it := range items {
		dup := false
		for _, e := range s.items {
			if e.TaskName == it.TaskName && e.ObjectId == it.ObjectId &&
				(e.Status == store.StatusPending || e.Status == store.StatusInProgress) {
				dup = true
			}
		}
		if !dup {
			c := *it
			s.items = append(s.items, &c)
			n++
		}
	}
	return n, nil
}

func (s *storeFake) GetById(_ context.Context, id string) (*store.WorkItem, *core.Error) {
	for _, it := range s.items {
		if it.Id == id {
			return it, nil
		}
	}
	return nil, core.NotFoundError()
}

func (s *storeFake) List(_ context.Context, taskName, status string, pg *page.Paging, _ page.SortRequest) ([]*store.WorkItem, *core.Error) {
	if s.err != nil {
		return nil, s.err
	}
	var all []*store.WorkItem
	for i := len(s.items) - 1; i >= 0; i-- { // dal più recente
		it := s.items[i]
		if it.TaskName == taskName && it.Status == status {
			all = append(all, it)
		}
	}
	start := (pg.CurrentPage - 1) * pg.PageSize
	if start >= len(all) {
		return nil, nil
	}
	return all[start:min(start+pg.PageSize, len(all))], nil
}

// add inserisce un item nello stato dato, creato a `at`.
func (s *storeFake) add(taskName, objectID, status string, at time.Time) *store.WorkItem {
	it := &store.WorkItem{Id: objectID + "-" + status, TaskName: taskName, ObjectId: objectID, Status: status, CreateTime: at, Error: "boom"}
	s.items = append(s.items, it)
	return it
}

// feederFake simula il feed S3: accoda nello store gli "arrivati" e restituisce l'esito.
type feederFake struct {
	st      *storeFake
	files   []string // file presenti nel bucket
	err     *core.Error
	calls   int
	clock   func() time.Time
	sources map[string]s3.Source
}

func (f *feederFake) Feed(ctx context.Context, taskName string) (s3.FeedResult, *core.Error) {
	f.calls++
	if f.err != nil {
		return s3.FeedResult{}, f.err
	}
	var items []*store.WorkItem
	now := f.clock()
	for _, k := range f.files {
		items = append(items, &store.WorkItem{Id: "item-" + k, TaskName: taskName, ObjectId: k, Status: store.StatusPending, CreateTime: now})
	}
	n, _ := f.st.InsertIfNotActive(ctx, items)
	return s3.FeedResult{Found: len(f.files), Queued: n}, nil
}

func (f *feederFake) Sources() map[string]s3.Source { return f.sources }

// outcomeFake è l'OutcomeReader di un'app con un registro degli esiti.
type outcomeFake struct {
	result string
	ok     bool
}

func (o outcomeFake) Outcome(context.Context, *store.WorkItem) (string, string, map[string]int64, bool) {
	return o.result, "dal registro", map[string]int64{"righe": 3}, o.ok
}

var t0 = time.Date(2026, 11, 12, 22, 0, 0, 0, time.UTC)

func testTasks() []task.Config {
	return []task.Config{
		{Name: "selftest", Type: "SELFTEST"},
		{Name: "cflow", Type: "CFLOW", S3: &s3.Source{Service: "s3", DestPath: "done"}},
	}
}

const token = "segreto-condiviso"

func newTestTrigger(st *storeFake, feeder *feederFake, outcome OutcomeReader) *Trigger {
	now := t0
	clock := func() time.Time { return now }
	if feeder != nil {
		feeder.st, feeder.clock = st, clock
	}
	var f s3.Feeder
	if feeder != nil {
		f = feeder
	}
	tr, err := newTrigger(&Config{Token: token}, testTasks(), st, f, outcome)
	if err != nil {
		panic(err)
	}
	tr.now = clock
	return tr
}

const bearer = "Bearer " + token
