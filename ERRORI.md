# Errori di go-core-trigger

Tutti gli errori nati qui hanno `Ambit = go-core-trigger` e la forma `DefaultError` di go-core-api
(`ambit`, `code`, `message`). Le costanti stanno in `errors.go`.

| Codice | HTTP | Costante | Quando |
|---|---|---|---|
| `TRIGGER-AUTH-001` | 401 | `CodeTokenMissing` | `Authorization` assente o non `Bearer <token>` |
| `TRIGGER-AUTH-002` | 401 | `CodeTokenInvalid` | token diverso da quello condiviso (`services.trigger.token`) |
| `TRIGGER-AUTH-003` | 403 | `CodeTaskDenied` | task dichiarato ma non fra quelli ammessi (`services.trigger.tasks`) |
| `TRIGGER-TASK` | 404 | `CodeUnknownTask` | task non dichiarato in `tasks:` |
| `TRIGGER-RUN` | 404 | `CodeUnknownRun` | `runId` malformato, o work item della run inesistente (cancellato dalla retention) |
| `TRIGGER-START` | 500 | `CodeStart` | esecuzione attiva non trovata dopo la deduplica (due tentativi): un'anomalia dello store |

Arrivano invariati da go-core-batch, con il loro ambito:

- `BATCH-S3-LIST` (500): l'elenco del bucket è fallito all'avvio di una run S3. Durante la run lo
  stesso guasto non è un errore: la run resta `RUNNING` con il messaggio e si riprova alla lettura
  successiva;
- gli errori dello store (`BATCH-INSERT`, `BATCH-LIST`, …).

Non sono errori, ma esiti di una run conclusa (`result` + `exitCode`): `SUCCESS` 0, `ERROR` 1,
`NOTHINGTODO` 4, `SYSTEMBUSY` 15.
