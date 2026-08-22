# Roadmap di costruzione — Quorum-Based Total Order Broadcast

Documento operativo che definisce tutti gli sprint e le fasi necessarie a
completare il progetto. È volutamente in italiano; identificatori, log e
report restano in inglese come da convenzione del corso.

> **Stato al 2026-08-22** (base `d854f59` + le correzioni dell'audit di §5.4).
> Sprint 0-4, 5.2 e 5.4 **completati**. `./gradlew build` verde,
> **98 test su 98 passati** in due esecuzioni consecutive, zero flakiness; i
> quattro scenari di `./gradlew run` girano end-to-end.
> **Resta solo lo Sprint 5.1 (report LaTeX) e lo Sprint 5.3 (consegna).**
>
> Per la descrizione di *come funziona il codice* — file per file, classe per
> classe, metodo per metodo — vedi
> [`IMPLEMENTATION_STATUS.md`](IMPLEMENTATION_STATUS.md).

## Documenti sorgente

Tutti i requisiti vengono da questi documenti (`docs/`):

- **`docs/ds1_project_2026.pdf`** — traccia ufficiale. Sezione 1 (descrizione
  protocollo), Sezione 2 (vincoli implementativi), Sezione 3 (report),
  Sezione 4 (presentazione e consegna).
- **`docs/ds1_project_2026_presentation.pdf`** — slide di progetto con la
  lista di domande di esempio cui il report deve rispondere
  (rif. traccia §3: "refer to the project presentation slides").
- **`docs/PROGETTO SD.pdf`** — note di pianificazione interne (design
  preliminare, scelte di scoping). Source-of-truth complementare alle slide.
- **Codebase obbligatoria** — clone di
  `github.com/StefanoGenettiUniTN/ds-student-project-2026` già integrato in
  `src/main/java/it/unitn/ds/` e `src/test/`. Le regole API che NON si possono
  violare sono raccolte in `~/.claude/.../project_codebase_rules.md` (memoria).

## Obiettivo finale (traccia §1)

Un sistema di N repliche Akka che mantiene un array di interi `P[]`
(`POSITIONS_LIST_LENGTH = 100`); client esterni leggono/scrivono contattando
una replica qualunque; le scritture passano per un coordinatore via
**two-phase total order broadcast** (`UPDATE` → quorum di `ACK` → `WRITEOK`);
in caso di crash del coordinatore una **ring election** elegge il nuovo, che
**sincronizza** prima di accettare nuovi update. Garanzia di safety: se una
replica applica `w`, ogni replica corretta finirà per applicare `w`
(traccia §1 — "Properties").

Quorum: `|Q| = ⌊N/2⌋ + 1`. Si assume FIFO + reliable channel emulato con
latenza random in `[MIN_LATENCY, MAX_LATENCY]` da `NetworkChannel`.

## Vincoli trasversali (sempre validi)

Devono valere in ogni sprint — non aggiungerli mai "alla fine".

- **Codebase obbligatoria**: estendere `Replica`/`Client` forniti, partire
  da `createBaseReceiveBuilder()`, inviare con `this.tell(Serializable, ActorRef)`
  (canale FIFO), MAI `getSelf().tell(...)`.
- **Logging**: solo `Logger.log` / `this.log()` — vietato `System.out.println`
  (traccia §2: "if prints are enabled during automated tests, they can
  interfere with their outcome").
- **Encapsulation**: niente stato mutabile condiviso fra attori; ogni
  messaggio inviato attraverso la rete deve essere `Serializable` e immutabile
  (traccia §2: "Shared mutable state is forbidden; any shared objects must be
  immutable").
- **Callback obbligatorie**:
  - `callbackOnUpdateApplied(idx, val)` — una volta per write applicata, su
    ogni replica.
  - `callbackOnElectionStarted(crashedCoordId)` — al massimo una volta per
    elezione per replica, alla prima `ELECTION` inviata.
  - `callbackOnCoordinatorElected(newCoordId)` — sul vincitore quando decide,
    su ogni altra replica al processamento della `Synchronization`.
  - `callbackOnReadResult` / `callbackOnWriteResult` / `callbackOn{Read,Write}Timeout`
    sul `Client`.
- **Format log** (traccia §2): timestamp ms-precision già gestito da
  `Logger`; il contenuto deve seguire i pattern:
  - `[Client X] requesting WRITE (idx, val) to ReplicaID`
  - `[Replica id] applied update <epoch>:<seq> (idx, val)`
  - `[Replica id] CRASHED`
  - ecc.
- **Crashed mode** (traccia §2): non chiamare `getContext().stop(getSelf())`
  — l'attore deve restare vivo ma ignorare tutto.

---

## Sprint 0 — Setup repository  ✅ COMPLETATO (2026-05-27)

- Riorganizzazione: `docs/`, `report/`, `src/main/java/it/unitn/ds/`,
  `src/test/java/it/unitn/ds/base/`.
- Gradle wrapper 9.2.1, dipendenze Akka 2.6 + JUnit 5.
- README di progetto.
- Git init + push su `git@github.com:Teto03/Distributed_Denial_of_Systems.git`.

**Exit criteria**: `./gradlew build` verde a partire da clone pulito.

---

## Sprint 1 — Modello dati + happy path  ✅ COMPLETATO

Obiettivo: sistema funzionante quando NESSUNO crasha. Niente heartbeat,
niente elezione.

### 1.1 Modello dati  ✅ COMPLETATO

Files in `src/main/java/it/unitn/ds/`:

- `UpdateID.java` — coppia immutabile `<epoch, sequence>`, `Comparable`.
  Riferimento traccia §1 "Update protocol": *"Each update is uniquely
  identified by a pair ⟨e, i⟩"*.
- `Update.java` — `(UpdateID id, int index, int value)` immutabile.
- `UpdateHistory.java` — log append-only con `latest()`, `latestId()`,
  `after(threshold)`. Servirà sia per la scelta del nuovo coordinatore
  (traccia §1 "Coordinator election": *"the replica that knows the most
  recent update"*) sia per la fase di Synchronization.

### 1.2 Messaggi di protocollo  ✅ COMPLETATO

Tutti `Serializable` e immutabili, in `src/main/java/it/unitn/ds/messages/`.
Il broadcast del coordinatore è nominato `UpdateMsg` per non collidere col
data class `Update` (history entry).

- Client→Replica: `ReadRequest`, `WriteRequest` (già nel codebase).
- Replica→Coordinatore: `ForwardWrite(idx, val, clientRef, contactedReplicaId)`.
  Il campo `contactedReplicaId` è critico per la regola "`fromReplica` =
  contacted replica" (regola 11 della codebase).
- Coordinatore→Repliche: `Update(UpdateID id, int idx, int val,
  ActorRef client, int contactedReplicaId)`.
- Replica→Coordinatore: `UpdateAck(UpdateID id)`.
- Coordinatore→Repliche: `WriteOk(UpdateID id)`.
- Coordinatore→Repliche: `Heartbeat()` (stub in Sprint 1, attivo da
  Sprint 2).
- Elezione: `Election(...)`, `ElectionAck(...)`, `Synchronization(...)`
  — definite ora ma gestione vera in Sprint 3.
- Timeout interni (a sé stesso): `UpdateTimeout`, `ForwardTimeout`,
  `HeartbeatTimeout`, `ElectionAckTimeout`, `GlobalElectionTimeout`.

### 1.3 Client e Replica — happy path  ✅ COMPLETATO

Quattro nuovi messaggi in `messages/` per il dialogo client ↔ replica
(`ClientRead`, `ClientWrite`, `ReadReply`, `WriteReply`): le
`AbstractClient.ReadRequest`/`WriteRequest` del codebase sono solo i messaggi
che il test harness invia al client, servivano quindi messaggi propri.
`reqId` (id di richiesta locale al client) aggiunto anche a `ForwardWrite` e
`UpdateMsg` per accoppiare risposta/timeout alla richiesta.

- `Replica.initSystem(InitSystem)` salva `group` e `coordinatorId`;
  `positions[POSITIONS_LIST_LENGTH]` parte a 0.
- `Replica` su read (`ClientRead`): risponde subito con
  `ReadReply(value=P[idx], fromReplica=self.id)`.
- `Replica` non-coordinatore su write (`ClientWrite`): forward `ForwardWrite`
  al coordinatore.
- `Coordinatore` su `ClientWrite`/`ForwardWrite`: assegna `<epoch, seq+1>`,
  broadcast `UpdateMsg` a tutte le repliche (sé incluso), conta gli
  `UpdateAck` e al quorum `⌊N/2⌋+1` broadcast `WriteOk`.
- `Replica` su `UpdateMsg`: `UpdateAck` al coordinatore + memorizza il
  proposal fino al `WriteOk`.
- `Replica` su `WriteOk`: applica `P[idx]=val`, append a `UpdateHistory`,
  logga `applied update <e>:<i> (idx, val)`, chiama `callbackOnUpdateApplied`.
  La sola replica contattata risponde al client con `WriteReply`
  (→ `WriteResult.fromReplica` = replica contattata, regola 11).
- `Client`: `sendRead`/`sendWrite` con timeout self-schedulati; le reply
  fanno fire `callbackOn{Read,Write}Result`; il tick fa fire
  `callbackOn{Read,Write}Timeout` solo se la reply non è ancora arrivata
  (matching per `reqId`).

> **Nota di scoping.** `ForwardTimeout`/`UpdateTimeout` lato replica NON sono
> ancora schedulati: hanno senso solo con i crash (rilevamento del
> coordinatore), quindi vengono attivati in Sprint 2. Il timeout lato client
> copre già il caso "nessuna risposta" osservabile in Sprint 1.

**Exit criteria** (tutti verdi, stabili su 3 run):
- `./gradlew test --tests "*APICompliance*oneClientWriteWaitRead*"` ✅ (4/4).
- `./gradlew test --tests "*APICompliance*callbackOnUpdateApplied*"` ✅ (6/6).
- `./gradlew test --tests "*NoCrashes*"` ✅ (4/4).

---

## Organizzazione del lavoro a due (Sprint 2–3)

Sprint 2 e Sprint 3 sono **sequenziali sul percorso critico** (il trigger
dell'elezione e la Synchronization dipendono dal crash detection e dalla FSM
dello Sprint 2). Per lavorare in parallelo NON ci si divide "uno sprint a
testa" — B resterebbe bloccato ad aspettare A. Ci si divide invece **per
modulo lungo l'interfaccia**, con tag di ownership su ogni task:

- **[A] — Detection & FSM**: possiede lo Sprint 2 completo (percorso critico).
- **[B] — Election & Sync (statico)**: possiede tutte le parti dello Sprint 3
  che sono *codice puro* e non richiedono runtime (POJO messaggi, logica ring,
  comparatore vincitore) + i relativi unit test isolati. Non dipende da A una
  volta fissato il contratto in Fase 0.
- **[A+B] — Integrazione**: il pezzo intrinsecamente sequenziale (aggancio
  timeout→elezione, Synchronization, anti-livelock) si fa in pair programming
  quando A ha finito lo Sprint 2.

### Fase 0 — Contratto delle interfacce  ✅ COMPLETATA E CHIUSA

> **Nota storica.** Questo era il contenuto del file `CONTRACT_PHASE0.md`, che
> congelava i confini fra i due flussi di lavoro *prima* di scrivere codice di
> logica. Il contratto è stato onorato per intero e le cinque decisioni aperte
> sono state chiuse dall'implementazione, quindi il documento separato è stato
> **assorbito qui** ed eliminato dal repository. Le annotazioni "esito"
> registrano ciò che il codice fa oggi.

Mezza giornata a tavolino per fissare i confini fra i due flussi, così A e B
lavorano su file diversi senza conflitti di merge.

#### F0.1 Mappa di ownership dei file

Il vincolo anti-conflitto era: **A e B non toccano mai lo stesso file** durante
il lavoro parallelo. L'unico file condiviso (`Replica.java`) era di A fino al
merge; B ci ha messo le mani solo in pair durante l'integrazione `[A+B]`.

| File / package                                         | Owner | Fase                     |
|--------------------------------------------------------|-------|--------------------------|
| `Replica.java`                                          | **A** | Sprint 2 + merge `[A+B]` |
| `messages/Heartbeat`, `HeartbeatTimeout`                | **A** | Sprint 2                 |
| `messages/UpdateTimeout`, `ForwardTimeout`              | **A** | Sprint 2                 |
| `election/` (package)                                   | **B** | Sprint 3 statico         |
| `messages/Election`, `ElectionAck`                      | **B** | Sprint 3                 |
| `messages/Synchronization`                              | **B** | Sprint 3                 |
| `messages/ElectionAckTimeout`, `GlobalElectionTimeout`  | **B** | Sprint 3                 |
| `src/test/.../election/` (unit test puri)               | **B** | Sprint 3 statico         |
| `AbstractReplica.java`, `AbstractClient.java`           | **—** | **codebase, INTOCCABILE** |
| `UpdateID`, `Update`, `UpdateHistory`                   | **—** | congelati in Fase 0      |

`AbstractReplica`/`AbstractClient` sono della codebase obbligatoria
Genetti/Pasquali: **non si modificano**. Se serve un metodo lì dentro, la
soluzione è un helper in `Replica` (A) o in `election/` (B).

#### F0.2 Catalogo messaggi — congelato

Firme definitive (in `src/main/java/it/unitn/ds/messages/`). Tutti
`Serializable` e immutabili.

| Messaggio               | Campi                                                                | Owner |
|-------------------------|----------------------------------------------------------------------|-------|
| `Heartbeat`             | *(nessuno)*                                                          | A |
| `HeartbeatTimeout`      | *(nessuno)* — self-message                                           | A |
| `UpdateTimeout`         | `UpdateID id` — self-message della replica in fase-1                 | A |
| `ForwardTimeout`        | `long reqId`, `int index`, `int value` — self-message dopo il forward | A |
| `Election`              | `int initiatorId`, `Map<Integer,UpdateID> latestPerReplica`          | B |
| `ElectionAck`           | *(nessuno)* — vedi D1                                                | B |
| `ElectionAckTimeout`    | `int successorId` — self-message                                     | B |
| `GlobalElectionTimeout` | *(nessuno)* — self-message                                           | B |
| `Synchronization`       | `int newCoordinatorId`, `int newEpoch`, `List<Update> pendingUpdates` | B |

Semantica congelata dei due messaggi non ovvi:

- **`Election.latestPerReplica`**: ogni replica che gestisce il messaggio
  inserisce la propria entry `id -> latestId` e forwarda al successore. Valore
  per una replica con history vuota = `new UpdateID(0, 0)` (sentinella, D2).
- **`Synchronization`**: inviato dal vincitore in broadcast. `pendingUpdates` =
  update orfani che ogni destinatario deve applicare *prima* di adottare
  `newEpoch`; `newEpoch` è calcolato dal vincitore (D3).

#### F0.3 Enum `Crash.Type` — congelato

Definito in `AbstractReplica.Crash.Type`, **già completo** nella codebase e
quindi intoccabile:

```
Now, Heartbeat, Update, WriteOK, Election
```

Semantica (regola 10 della codebase): `Crash(type, after_n_messages_of_type)` =
processa `n` messaggi di quel tipo, **crasha all'(n+1)-esimo**. `Now` = crash
immediato. Il conteggio per tipo è interno ad A (`checkCrashCondition` +
`become(crashed())`); a B bastava sapere che `Election` è un punto di crash
valido durante l'elezione.

#### F0.4 Contratto della FSM (stati e `become`) — owner A

| Stato | Metodo behavior | Messaggi gestiti |
|-------|-----------------|------------------|
| `NORMAL`   | `createReceive()` | base (`Crash`, `InitSystem`) + `ClientRead/Write`, `ForwardWrite`, `UpdateMsg`, `UpdateAck`, `WriteOk`, `Heartbeat`, `HeartbeatTimeout`, `UpdateTimeout`, `ForwardTimeout` |
| `ELECTION` | `election()`      | `Crash` + `Election`, `ElectionAck`, `ElectionAckTimeout`, `GlobalElectionTimeout`, `Synchronization`; write in ingresso **bufferizzate** (D4) |
| `CRASHED`  | `crashed()`       | **nessuno** — `receiveBuilder().matchAny(ignore).build()`; **mai** `getContext().stop()` |

Regole congelate, tutte rispettate:

- transizioni via `getContext().become(...)`: `NORMAL → CRASHED` (crash),
  `NORMAL → ELECTION` (heartbeat/forward/update timeout o ricezione di
  `Election`), `ELECTION → NORMAL` (vittoria o `Synchronization` processata),
  `* → CRASHED`;
- `crashed()` **non** parte da `createBaseReceiveBuilder()` (altrimenti
  ri-gestirebbe `Crash`): è un receive che ignora tutto;
- l'invio è **sempre** `this.tell(Serializable, ActorRef)` (canale FIFO
  emulato). **Mai** `getSelf().tell(...)` né `getContext().stop()`.

**Esito**: rispettato integralmente. In più, in `NORMAL` sono agganciati anche
i messaggi di elezione, perché una replica che non ha ancora rilevato il crash
può essere trascinata in un round o apprenderne l'esito senza passare da
`ELECTION`.

#### F0.5 Contratto dei timeout — self-scheduled

| Timeout                 | Schedulato da                    | Durata concordata                     | Owner | Esito |
|-------------------------|----------------------------------|---------------------------------------|-------|-------|
| `HeartbeatTimeout`      | replica non-coordinatrice        | `~3 × getCoordinatorBeatInterval()`   | A | ✅ `3 × beat` |
| `UpdateTimeout`         | replica in fase-1                | `getMaxLatencyPlusTolerance()`        | A | ⚠️ implementato a `3 × beat`: più conservativo, evita falsi positivi |
| `ForwardTimeout`        | replica dopo il forward al coord | `getMaxLatencyPlusTolerance()`        | A | ⚠️ idem, `3 × beat` |
| `ElectionAckTimeout`    | replica che forwarda `Election`  | `getMaxLatencyPlusTolerance()`        | B | ✅ |
| `GlobalElectionTimeout` | replica in `ELECTION`            | `N × ElectionAckTimeout`              | B | ✅ `N × maxLatencyPlusTolerance × 2` |

Pattern condiviso e rispettato: ogni timer per-scopo sta in un `Cancellable`
(o in una `Map<chiave, Cancellable>` per quelli per-update / per-richiesta) e
viene **cancellato** all'arrivo della risposta attesa, per non far scattare
falsi positivi.

#### F0.6 API di confine fra A e B — congelata

Riusate dal modello dati esistente, senza lavoro aggiuntivo:

```java
Optional<UpdateID> UpdateHistory.latestId();  // per riempire Election.latestPerReplica
List<Update>       UpdateHistory.after(UpdateID t);  // diff di Synchronization
int      UpdateID.compareTo(UpdateID other);  // ordine lessicografico <epoch,seq>
```

Callback obbligatorie di `AbstractReplica` (timing in F0.8):

```java
void callbackOnElectionStarted(int crashedCoordinatorId);
void callbackOnCoordinatorElected(int newCoordinatorId);
void callbackOnUpdateApplied(int index, int value);
```

Nuove classi di B, package `it.unitn.ds.election` — logica pura, senza attori,
unit-testabile in isolamento. Firme congelate in Fase 0 come stub che
lanciavano `UnsupportedOperationException`, riempite in Sprint 3:

```java
RingTopology.order(Collection<Integer> memberIds) -> List<Integer>
RingTopology.successor(int self, Collection<Integer> memberIds, Set<Integer> suspected) -> Optional<Integer>
ElectionLogic.NONE                                                   // sentinella (D2)
ElectionLogic.winner(Map<Integer,UpdateID> latestPerReplica) -> int
SyncPlan.missingFor(UpdateHistory winnerHistory, UpdateID recipientLatest) -> List<Update>
```

**Esito**: tutte e cinque implementate con la firma concordata. In corso d'opera
sono stati aggiunti tre metodi non previsti dal contratto, tutti puri e testati:
`ElectionLogic.latestOf`, `ElectionLogic.withEntry`, `ElectionLogic.newEpoch`,
più `SyncPlan.oldest` e `SyncPlan.missingForAll` (il broadcast unico calcolato
sul watermark).

Seam esposto da A e usato dall'integrazione:

```java
void startElection(int crashedCoordinatorId);   // Replica.java:485
```

#### F0.7 Le cinque decisioni D1–D5 — **tutte chiuse**

| # | Decisione | Raccomandazione di Fase 0 | Esito nel codice |
|---|-----------|---------------------------|------------------|
| D1 | correlazione di `ElectionAck` | tenerlo vuoto, scartare gli ack inattesi | ✅ **come raccomandato**: ack vuoto, hop-by-hop fra vicini |
| D2 | sentinella per history vuota | `NONE = new UpdateID(0,0)` | ✅ **come raccomandato** (`ElectionLogic.java:26`) |
| D3 | calcolo di `newEpoch` | `maxEpochVisto + 1`, dopo aver completato gli orfani | ✅ **come raccomandato**, con il massimo preso su **tutto** il payload e non solo sulla history del vincitore (`ElectionLogic.newEpoch`) |
| D4 | write in arrivo durante `ELECTION` | droppare, il client ritenta | ❗ **risolta al contrario**: il client di questo progetto **non ritenta mai**, quindi droppare significherebbe perdere la richiesta. Le write sono **bufferizzate** in `clientWrites` e rigiocate al nuovo coordinatore (`onClientWriteDuringElection`, `replayPendingClientWrites`) |
| D5 | sorgente del set `suspected` | coordinatore crashato + successori senza ack | ✅ **come raccomandato** (`Replica.suspected`, alimentato da `startElection` e `onElectionAckTimeout`) |

#### F0.8 Timing delle callback — congelato

- `callbackOnElectionStarted(crashedCoordId)` — **esattamente una volta per
  partecipazione all'elezione**, alla prima `Election` inviata da questa
  replica. Garantito dal flag `electionStartedFired`, che **non** viene
  riazzerato dal `GlobalElectionTimeout`.
- `callbackOnCoordinatorElected(newCoordId)` — sul **vincitore** quando decide
  di aver vinto, e su **ogni altra replica** al processamento della
  `Synchronization`.
- `callbackOnUpdateApplied(idx, val)` — una volta per write applicata, anche per
  gli update orfani riapplicati durante la sync. Invocata da un solo punto del
  codice (`applyUpdate`).

#### F0.9 Exit criteria della Fase 0 — tutti raggiunti

- [x] le 3 classi stub in `election/` create con le firme di F0.6;
- [x] gli stub dei test in `src/test/.../election/` creati;
- [x] le 5 decisioni D1–D5 chiuse (F0.7);
- [x] `./gradlew build` verde;
- [x] i test dello Sprint 1 non regrediti.

I due branch `sprint2-detection` e `sprint3-election` sono partiti dallo stesso
commit con contratto verde; `sprint3-election` è stato mergiato in `main` con
`6c01abe`.

---

## Sprint 2 — Heartbeat + crash semplici  ✅ COMPLETATO   👤 Owner: **A**

Obiettivo: il sistema sopravvive a crash isolati di repliche non-coordinatrici
e rileva la morte del coordinatore (senza ancora rieleggere).

Riferimento traccia §1 "Crash detection".

> **Divisione**: tutto lo Sprint 2 è di **[A]**. È il fondamento del detection
> e sta sul percorso critico: va coperto da una persona dedicata mentre B
> avanza in parallelo sulle parti statiche dello Sprint 3.

- `Heartbeat` periodico dal coordinatore ogni `getCoordinatorBeatInterval()`
  (default 1000 ms, regola 9 della codebase).
- Stato `CRASHED`: tramite `getContext().become(crashed())` con un
  `Receive` che droppa silenziosamente tutto (ma NON `stop`).
- Implementazione di `crash(Crash how_to_crash)` con contatori per
  `Crash.Type.{Now, Heartbeat, Update, WriteOK, Election}` — semantica
  "after_n_messages_of_type" = processa N, crasha al messaggio N+1
  (regola 10 codebase). `Now` crasha subito, ignorando il contatore: non c'è
  un tipo di messaggio da contare.
- Il contatore è condiviso fra le due direzioni: `broadcast(msg, crashPoint)`
  valuta la condizione **una volta per destinatario**, così `Crash(Update, n)`
  e `Crash(WriteOK, n)` sul coordinatore producono un broadcast **parziale**
  (serve n repliche e muore). È quello che serve per innescare la
  "partial dissemination" della traccia §1 "Properties".
- `HeartbeatTimeout`: scatta dopo `coordinatorBeatInterval × 3` e avvia
  direttamente l'elezione (l'aggancio è stato fatto nell'integrazione
  `[A+B]` dello Sprint 3, insieme a `ForwardTimeout` e `UpdateTimeout`).
- `ForwardTimeout` su write: il client riceve `WriteTimeout` se la replica
  contattata non riceve `WriteOk` entro la finestra.

**Exit criteria** — tutti raggiunti:
- ✅ `APICompliance.replicasCrashNow` verde (tutte le repliche reagiscono al
  `Crash(Now, 0)`).
- ✅ `APICompliance.crashReplicaAndTryRequests` verde (client va in
  `ReadTimeout`/`WriteTimeout` quando la replica contattata è crashed).
- ✅ `WithCrashes.nonCoordinatorsCrashClientWritesWaitsReads` verde per
  N ∈ {7, 22}: crashano `N/2 − 2` repliche non coordinatrici, la write passa
  lo stesso e la read successiva vede il valore.

---

## Sprint 3 — Elezione + sincronizzazione  ✅ COMPLETATO

Obiettivo: dopo il crash del coordinatore, le repliche eleggono un nuovo
coordinatore via ring e completano gli update pendenti prima di accettarne
nuovi.

Riferimento traccia §1 "Coordinator election" + "Properties" (uniform
agreement).

> **Divisione del lavoro** (tag su ogni task):
> - **[B]** = codice puro, parallelizzabile fin da subito (nessun runtime,
>   testabile in isolamento). ~60% dello Sprint 3.
> - **[A+B]** = integrazione con la FSM/detection dello Sprint 2, da fare in
>   pair programming quando A ha finito. È la parte delicata sulle invarianti
>   (TO-1 / uniform agreement).

**Parti statiche — [B] (in parallelo con lo Sprint 2 di A):**

- **[B] Ring topology**: ordine crescente di `id`, il successore di `i` è
  `(i+1) mod N` skip-ando le repliche note come crashed. Funzione pura →
  unit test dedicato.
- **[B] Messaggio Election**: porta `Map<ReplicaId, UpdateID>` con il
  `latestId` noto a ciascuna replica (oppure lista di entries — scelta da
  motivare in report). Acked hop-by-hop con `ElectionAck` (traccia §1). POJO
  immutabile.
- **[B] Decisione del vincitore**: replica con `latestId` massimo; tie-break
  per `id` più alto (regola di codice). Comparatore isolato → unit test su
  casi limite (parità, history vuota).
- **[B] Diff della Synchronization**: dato il `latestId` di una replica,
  calcolare la lista di `Update` mancanti dalla history del vincitore.
  Logica pura su `UpdateHistory`, testabile senza attori.

**Integrazione — [A+B] (dopo lo Sprint 2, in pair):**

- **[A+B] Receive separato per ELECTION**: durante l'elezione la replica passa
  a un behavior dedicato che gestisce solo `Election`, `ElectionAck`,
  `Synchronization`, `Crash`, `ElectionAckTimeout`,
  `GlobalElectionTimeout`. Tutte le `Update`/`WriteOk` ricevute in questo
  stato vengono accodate o droppate secondo specifica. (Richiede la FSM
  `become` dello Sprint 2 → [A].)
- **[A+B] Trigger dell'elezione**: collegare l'`HeartbeatTimeout` dello
  Sprint 2 (finora logging-only) all'avvio effettivo dell'elezione.
- **[A+B] `ElectionAckTimeout`**: se il successore non ACKa, skip e forward al
  successivo (traccia §1: *"a replica that forwards an ELECTION message
  starts a timeout while waiting for the corresponding ACK"*).
- **[A+B] `Synchronization` broadcast**: il vincitore broadcasta annuncio +
  lista degli `Update` mancanti per ciascuna replica (usa il diff di [B]).
- **[A+B] Completamento update pendenti** prima di bumpare l'epoch: la traccia
  §1 "Properties" lo richiede esplicitamente. Solo dopo aver "chiuso" gli
  update orfani il vincitore incrementa l'epoch via `nextEpoch()` e
  riprende le write.
- **[A+B] `GlobalElectionTimeout`**: rete di sicurezza contro livelock — se
  l'elezione non termina entro N×ElectionAckTimeout, riparte.
- **[A+B] Firing callback**: `callbackOnElectionStarted(crashedCoordId)` alla
  prima `Election` inviata; `callbackOnCoordinatorElected(newCoordId)` sul
  vincitore alla decisione e su ogni altra replica al processamento di
  `Synchronization` (regola 6 codebase + commenti nei test).

**Punto di merge**: fatto. Il branch `sprint3-election` è stato mergiato in
`main` (`6c01abe`) e l'integrazione `[A+B]` è entrata con i due commit
successivi.

**Exit criteria** — tutti raggiunti:
- ✅ `APICompliance.callbackOnElectionStartedInvokedCorrectly` verde per
  N ∈ {5,7}.
- ✅ `APICompliance.callbackOnElectionStartedCalledAtMostOncePerReplica`
  verde.
- ✅ `APICompliance.callbackOnCoordinatorElectedAllAgree` verde.
- ✅ `APICompliance.callbackOnCoordinatorElectedNewCoordAlsoCalls` verde.
- ✅ `WithCrashes.coordinatorCrashClientWritesWaitsReads` verde per
  N ∈ {7, 22} con coordinatore + 2 repliche crashate.

**Aggiunte rispetto al piano iniziale**, emerse durante l'integrazione:

- **Buffer delle write durante l'elezione** (`clientWrites` +
  `replayPendingClientWrites`). Il client emette ogni richiesta una sola volta
  e non ritenta: senza buffer, una write il cui coordinatore muore a metà
  andrebbe persa. Le write che arrivano in stato `ELECTION` sono parcheggiate
  (decisione D4) e rigiocate verso il nuovo coordinatore.
- **Guardia sulle `Election` stantie** (`isStaleElection`): un messaggio
  ritardatario di un round già deciso eleggerebbe il coordinatore che stiamo
  già seguendo, quindi va scartato invece di far ripartire un sistema ormai
  stabile.
- **Idempotenza della `Synchronization`**: la stessa annuncio può tornare più
  volte, perché il nuovo coordinatore risponde con una `Synchronization` a
  ogni `Election` ritardataria. Rieseguire l'handler rigiocherebbe le write
  bufferizzate una seconda volta, trasformando una richiesta del client in due
  update: il duplicato viene riconosciuto su `(newCoordinatorId, newEpoch)` e
  ignorato.

---

## Sprint 4 — Corner case del fault model  ✅ COMPLETATO

Obiettivo: tutti i test del codebase (`NoCrashes`, `WithCrashes`,
`APICompliance`) verdi, inclusi gli scenari che la traccia §1 "Properties"
chiama "partial dissemination", **più** una suite dedicata ai punti di crash
che la traccia §2 chiede di poter innescare (*"trigger crashes at specific
points in the protocol execution"*).

L'istrumentazione che li rende innescabili è `broadcast(msg, crashPoint)`
(`Replica.java:785`), che valuta la condizione di crash **una volta per
destinatario** e produce quindi broadcast parziali riproducibili.

La suite è in `src/test/java/it/unitn/ds/scenarios/CornerCases.java`
(5 casi end-to-end). Ogni scenario mantiene viva una maggioranza stretta, come
impone il modello di guasto.

| # | Caso | Come si innesca | Stato |
|---|------|-----------------|-------|
| 1 | Coordinatore crasha **durante il broadcast di UPDATE** | `Crash(Update, 2)`, N=5 | ✅ `coordinatorCrashesDuringUpdateBroadcast` |
| 2 | Coordinatore crasha **dopo WRITEOK ad alcuni** — uniform agreement | `Crash(WriteOK, 2)` | ✅ `coordinatorCrashesDuringWriteOkDissemination` + Demo 4 |
| 3 | **Due nodi consecutivi** crashano durante l'elezione | `Crash(Now, 0)` su due id adiacenti, N=7 | ✅ `twoConsecutiveReplicasCrashDuringElection` |
| 4 | Vincitore crasha **prima della Synchronization** | `Crash(Election, 1)` sul futuro vincitore | ✅ `electionWinnerCrashesBeforeSynchronization` |
| 5 | Replica crasha **dopo l'ACK**, prima di applicare | `Crash(WriteOK, 0)` su una non-coordinatrice | ✅ `replicaCrashesAfterAckBeforeApplying` |
| 6 | Client contatta una replica crashed | `Crash(Now, 0)` + richiesta | ✅ `APICompliance.crashReplicaAndTryRequests` |

Il caso 4 è quello che giustifica il `GlobalElectionTimeout`: se il vincitore
muore prima di annunciarsi, nessun altro meccanismo sbloccherebbe il sistema.
Il caso 5 verifica anche il negativo — che **nessuna elezione venga avviata**
quando il coordinatore è vivo.

**Exit criteria** — tutti raggiunti:
- ✅ `./gradlew test` interamente verde: **98/98**, su due run consecutivi;
- ✅ suite dedicata ai casi 1-5 in `src/test/java/it/unitn/ds/scenarios/`.

---

## Sprint 5 — Report, demo e consegna  ⚠️ IN CORSO (unico sprint aperto)

Riferimento traccia §3 (report) e §4 (presentation & submission).

### 5.1 Report LaTeX  ❌ DA FARE — **unico blocco alla consegna**

- Template fornito (linkato in traccia §3) — già presente in `report/`.
- 3-4 pagine, **max 6** ("Reports exceeding this page limit will be
  automatically rejected").
- Inglese.
- Coprire le domande di esempio della slide `docs/ds1_project_2026_presentation.pdf`:
  scelte architetturali, gestione timeout, scelta della topologia del ring,
  trattamento degli update orfani, motivazioni di tie-break, assunzioni
  aggiuntive.
- **Stato attuale `report/`**: `main.tex` compila ma le tre sezioni
  (`01_structure.tex`, `02_design.tex`, `03_implementation.tex`) sono file di
  **una sola riga** con il solo `\section{}`; anche
  `\author{Surname1 Name1, Surname2 Name2}` è ancora il placeholder.
- Materiale già pronto da cui attingere: `IMPLEMENTATION_STATUS.md` §9 (anatomia
  di `Replica`), §15 (dimostrazione delle quattro proprietà), §16 (timer e
  perché niente falsi positivi), §22 (**l'elenco completo delle assunzioni da
  dichiarare**, che la traccia §2 richiede esplicitamente).
- Va incluso il disclaimer sull'uso di assistenza AI.

### 5.2 Demo scenarios  ✅ COMPLETATO

Traccia §4 raccomanda *"three or four representative execution examples,
including corner cases"*. Implementate in `Main.java`, una per metodo, ognuna
con il proprio `ActorSystem` creato e terminato, così che il log di uno
scenario si legga da solo. Tutto passa da `Logger` (niente `System.out`).

- **Demo 1** — happy path: 1 client su Replica 4, tre write e una read;
  si vedono gli id consecutivi `<0,1> <0,2> <0,3>` e l'ordine totale
  rispettato su tutte le repliche.
- **Demo 2** — crash di una replica non-coordinatrice: la write successiva
  raggiunge comunque il quorum (3 ack su 5), mentre il client attaccato alla
  replica morta va in `TIMEOUT READ`.
- **Demo 3** — crash del coordinatore: `HEARTBEAT TIMEOUT` → elezione ad
  anello → `SYNCHRONIZATION` → la write bufferizzata da Replica 2 viene
  rigiocata e completata in epoch 1 (`<1,1>`).
- **Demo 4** — corner case della safety: `Crash(WriteOK, 2)` fa morire il
  coordinatore con il WRITEOK consegnato **a una sola replica**. Quella
  replica è l'unica ad aver applicato `<0,1>`, vince l'elezione perché
  conosce l'update più recente e lo rimette in circolo con la
  `Synchronization` — uniform agreement dimostrata dal vivo.

```bash
./gradlew run                 # tutti e quattro gli scenari in sequenza
./gradlew run --args="3"      # solo lo scenario 3
```

### 5.3 Checklist di consegna (traccia §4)

- [x] **Committare il wrapper Gradle.** Fatto il 2026-08-22: `.gitignore` non
  esclude più `gradlew`, `gradlew.bat` e `gradle/wrapper/`, che sono ora
  tracciati (`gradlew` con il bit di esecuzione). Da un clone pulito basta un
  JDK 17+ e `./gradlew build`.
- [ ] Verificare `./gradlew test` interamente verde su clone pulito
  (sul working tree: ✅ 98/98, due run, anche dopo le correzioni di §5.4).
- [ ] Report in formato `.pdf` autocontenuto, con i nomi veri al posto dei
  placeholder del template.
- [ ] Archivio `tar -czvf CognomeACognomeB.tgz CognomeACognomeB/` con dentro
  sorgenti + report (cartella con i due cognomi). **Non includere** le PDF
  del prof in `docs/`.
- [ ] Prenotare slot di presentazione via mail a Picco + Pasquali + Genetti
  prima della deadline del corrispondente slot.
- [ ] Indicare in-person vs online.
- [ ] 12 minuti di presentazione (timer rigido) + Q&A: usare le 4 demo di
  `Main` e i 5 corner case come traccia della dimostrazione.

### 5.4 Rifiniture emerse dall'audit  ✅ TUTTE APPLICATE (2026-08-22)

Quattro imperfezioni trovate rileggendo il codice contro la traccia. Nessuna
faceva fallire un test; sono state comunque risolte tutte, e la suite è stata
rieseguita due volte dopo le modifiche (**98/98 verdi**). Il dettaglio con
problema e rimedio è in `IMPLEMENTATION_STATUS.md` §21.

- [x] **Wrapper Gradle non tracciato** → committato, vedi §5.3.
- [x] **Nove refusi nei commenti** di `Replica.java` (`awating`,
  `callbackOnElectionSTarted`, `left aliv`, `havea lready`, `Puleld`) e di
  `CornerCases.java` (`succeded` ×2, `bufered`, `surviced`, `tio`) — la traccia
  §4 valuta anche la forma del codice.
- [x] **Doppio conteggio del crash counter sul coordinatore.** `broadcast`
  include il coordinatore fra i destinatari, quindi per `Update`/`WriteOK`/
  `Heartbeat` lo stesso messaggio veniva contato due volte, in violazione della
  semantica `after_n_messages_of_type` (regola 10 della codebase). Gli handler
  in ricezione passano ora da `checkIncomingCrashCondition`, che non riconteggia
  i broadcast propri: **un messaggio, un incremento**.
- [x] **`Election` ritardataria che rimetteva in elezione un coordinatore
  sano.** `isStaleElection` ora tratta come stantia ogni `Election` che
  raggiunge il coordinatore in carica — la traccia assume detection accurata,
  quindi un coordinatore vivo non è mai legittimamente sospettato — e risponde
  con una `Synchronization` invece di aprire un epoch in più; `onSynchronization`
  toglie il mittente da `suspected` (prova di vita). La regola della traccia
  *"if it is not already participating in the election, it adds its own
  information"* resta invariata per tutte le altre repliche.
- [x] ~~`CONTRACT_PHASE0.md` con le decisioni D1-D5 formalmente aperte~~ — il
  contratto è stato assorbito nella Fase 0 di questo documento (§F0.1-F0.9) con
  l'esito di ciascuna decisione annotato, e il file separato è stato eliminato.

---

## Tracciabilità requisiti traccia → sprint

Per il puntamento **riga per riga** al codice vedi
`IMPLEMENTATION_STATUS.md` §2.

| Requisito traccia                                                | Sprint | Stato |
|------------------------------------------------------------------|--------|-------|
| §1 Two-phase update (UPDATE/ACK/WRITEOK), quorum ⌊N/2⌋+1         | 1      | ✅ |
| §1 UpdateID ⟨epoch, seq⟩                                          | 1      | ✅ |
| §1 Read servita localmente, write inoltrata al coordinatore       | 1      | ✅ |
| §1 Heartbeat per liveness coordinatore                            | 2      | ✅ |
| §1 Crash detection via timeout (heartbeat, forward, update)       | 2      | ✅ |
| §1 Ring election, ACK hop-by-hop, skip su timeout                 | 3      | ✅ |
| §1 Vincitore = max latest update, tie-break id                    | 3      | ✅ |
| §1 Synchronization + completamento update pendenti                | 3      | ✅ |
| §1 Uniform agreement con crash del coord a metà broadcast         | 2,3,4  | ✅ |
| §2 Logging formattato e timestamped                               | 1+     | ✅ |
| §2 Crashed mode (no `stop`)                                       | 2      | ✅ |
| §2 Crash istrumentati per tipo di messaggio                       | 2      | ✅ |
| §2 Crash innescabili in punti specifici del protocollo            | 4      | ✅ |
| §2 FIFO + latenza random emulata (NetworkChannel)                 | dato   | ✅ |
| §2 Immutability dei messaggi inviati                              | 1+     | ✅ |
| §2 Sequenze di write + crash con read concorrenti                 | 1,4    | ✅ |
| §2 Assunzioni aggiuntive dichiarate                               | 5      | ⚠️ elencate in `IMPLEMENTATION_STATUS.md` §22, da riportare nel report |
| §3 Report 3-4 pagine LaTeX in inglese                             | 5      | ❌ |
| §4 Demo 3-4 scenari + 12 min presentation                          | 5      | ✅ demo pronte, presentazione da preparare |
