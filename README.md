# go-core-trigger

Attivazione dei task di [go-core-batch](../go-core-batch) dall'esterno: un'API da chiamare per far
partire un task e seguirlo fino all'esito. È pensata per un Job o CronJob Kubernetes (o per TWS) che
chiama l'applicativo con un token condiviso ed esce col codice dell'esito.

```go
// main.go
trigger.Module(&svc.Trigger, svc.Batch.TasksConfig)
coreapi.Module(&svc.Api, coreapi.WithRoutes(trigger.Register))
batch.Module(&svc.Batch, Register, …, batch.WithS3(s3feed.Module)) // solo se ci sono task con s3:
```

```go
// services/config.go
type Config struct {
    Api     coreapi.Config `yaml:"api"     mapstructure:"api"`
    Batch   batch.Config   `yaml:"batch"   mapstructure:"batch"`
    Trigger trigger.Config `yaml:"trigger" mapstructure:"trigger"`
}
```

## Il modello

**Il trigger di un task è il suo feed**, cioè ciò che lo mette in coda. Ad eseguire resta il job del
task (un `SingleTask` o un `DistribuiteTask` che gira ogni tot): il trigger accoda e osserva.

| Task | Il trigger… | La run è conclusa quando… |
|---|---|---|
| senza `s3:` (`kind: item`) | accoda un work item (`objectId` = nome del task, `payload` = `params`); se ce n'è già uno attivo ci si aggancia | il work item è DONE (SUCCESS) o FAILED (ERROR) |
| con `s3:` (`kind: s3`) | esegue il feed S3 del task (`s3.Feeder`): un work item per file del prefisso | il bucket è vuoto e nessun file è in coda (SUCCESS, o NOTHINGTODO se non c'era nulla), oppure un file della run è FAILED (ERROR) |

```yaml
tasks:
  - name: selftest
    type: SELFTEST
  - name: import
    type: IMPORT                       # runner.RegisterFile
    s3: { service: s3, path: "inbox/", pattern: "*", dest-path: "archive" }
jobs:
  - { name: selftest-exec, type: SingleTask, cron: "*/10 * * * * *", properties: { task: selftest } }
  - { name: import-exec,    type: SingleTask, cron: "*/30 * * * * *", lock-timeout: 3h, properties: { task: import } }
```

**La run S3 è "a finestra".** Ogni lettura della run (il long-poll di chi la segue) la fa avanzare:

1. oltre `run-ttl` → ERROR;
2. un file in coda o in lavorazione → RUNNING;
3. un file della run FAILED → ERROR. Non lo si riaccoda: il file è ancora nel prefisso, e la run
   successiva lo riprende;
4. altrimenti riesegue il feed: file arrivati nel frattempo → RUNNING, bucket vuoto → conclusa.

Con il bucket vuoto la POST risponde già conclusa `NOTHINGTODO`. Se nessuno segue la run, i file
accodati all'avvio (al più `s3.limit`) vengono comunque elaborati; quelli arrivati dopo li prende la
run successiva, o il cron di un job `FeedS3Task` se c'è.

**Lo stato non sta in una tabella.** Il `runId` codifica ciò che serve a ricalcolarlo dai work item
(`item:<workItemId>`, `s3:<task>:<istante in ms>`), quindi funziona identico sui tre backend di
go-core-batch e su più repliche. Due run sullo stesso task vedono gli stessi item e si agganciano di
fatto.

**Esiti applicativi.** Lo stato di un work item dice solo DONE o FAILED. Un'app che distingue
NOTHINGTODO o SYSTEMBUSY (un registro suo degli esiti) fornisce a fx un `trigger.OutcomeReader`, che
il trigger consulta per le run di un item. Un item FAILED resta ERROR in ogni caso.

## API

Tutte le rotte vogliono `Authorization: Bearer <token>`.

| Metodo | Path | Risposta |
|---|---|---|
| POST | `/api/v1/trigger/{task}` (body `{"params": {...}}` facoltativo) | **202** run avviata, **200** agganciata a una in corso o già conclusa; body `Run` |
| GET | `/api/v1/trigger/runs/{runId}?wait=N` | `Run`, dopo aver atteso al più `N` secondi (limitati da `max-wait`) che si concluda |
| GET | `/api/v1/trigger` | `[{task, kind}]`: i task attivabili |

```json
{ "runId": "s3:import:1794520800000", "task": "import", "kind": "s3", "state": "DONE",
  "result": "SUCCESS", "exitCode": 0, "message": "2 file elaborati", "counters": {"files": 2},
  "retry": 0, "attached": false }
```

Codici d'uscita (gli stessi di `invokeBatch.sh`): `SUCCESS` 0, `ERROR` 1, `NOTHINGTODO` 4,
`SYSTEMBUSY` 15. Gli errori sono in [ERRORI.md](ERRORI.md).

## Configurazione

```yaml
trigger:
  token: "${TRIGGER_TOKEN}"   # il token del Secret condiviso nel namespace (facoltativo solo in develop-mode)
  tasks: []                   # task attivabili; vuoto = tutti quelli di tasks:
  max-wait: 50s               # tetto del long-poll (sotto il timeout della Route)
  run-ttl: 12h                # durata massima di una run S3
```

**Token condiviso.** Un solo Secret nel namespace, con la chiave `token`. L'applicativo lo legge come
variabile d'ambiente (`TRIGGER_TOKEN`, da `secretKeyRef`); il Job/CronJob che chiama lo monta come
file. Il confronto avviene a tempo costante, sugli SHA-256 dei due valori.

**Token obbligatorio, salvo in develop-mode.** Fuori dal `develop-mode: true` di go-core-api, un token
assente fa panic in `Register`: l'applicativo non parte. Con `develop-mode: true` il token è
facoltativo: se manca l'autenticazione è spenta (con un Warn al boot), come le altre comodità di
sviluppo del develop-mode; se c'è, vale anche lì. Un segnaposto non risolto (`${TRIGGER_TOKEN}` con
la variabile non definita: la sostituzione della config lascia il letterale) conta come token assente,
altrimenti il token sarebbe la stringa scritta nel repository.

**Con go-core-auth.** Il middleware di `apiauth` nega le richieste senza ruoli, e i suoi
`guest-paths` sono path esatte, quindi non coprono `/api/v1/trigger/{task}`. Un'app che monta
entrambi oggi deve tenere il trigger fuori dal middleware: è un limite noto, da risolvere in
go-core-auth con i `guest-paths` per prefisso.

## Dipendenze

Il modulo importa go-core-app, go-core-api e, di go-core-batch, i soli `store`, `task` e `s3`, che non
portano l'SDK AWS. Il feed S3 arriva dal grafo come `s3.Feeder` (`batch.WithS3(s3feed.Module)`): se un
task ha `s3:` e il Feeder manca, l'avvio fallisce dicendolo.
