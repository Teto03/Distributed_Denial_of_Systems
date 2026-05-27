# Roadmap di costruzione — Quorum-Based Total Order Broadcast

Documento operativo che definisce tutti gli sprint e le fasi necessarie a
completare il progetto. È volutamente in italiano; identificatori, log e
report restano in inglese come da convenzione del corso.

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

## Sprint 1 — Modello dati + happy path

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

### 1.2 Messaggi di protocollo

Tutti `Serializable` e immutabili, in `src/main/java/it/unitn/ds/` (o
sotto-package `messages/` se preferito).

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

### 1.3 Client e Replica — happy path

- `Replica.initSystem(InitSystem)` salva `group`, `coordinatorId`, inizializza
  `positions[POSITIONS_LIST_LENGTH]`.
- `Replica` su read: risponde subito con `ReadResult(true, idx, P[idx],
  contactedReplicaId=self.id)`.
- `Replica` non-coordinatore su write: forward al coordinatore, parte
  `ForwardTimeout`.
- `Coordinatore` su forward: assegna `<epoch, seq+1>`, broadcast `Update`,
  attende quorum di `UpdateAck`, broadcast `WriteOk`. Su `WriteOk` ricevuto
  da sé stesso applica e fa fire della callback.
- `Replica` su `Update`: ack al coordinatore, parte `UpdateTimeout`.
- `Replica` su `WriteOk`: applica `P[idx]=val`, append a `UpdateHistory`,
  chiama `callbackOnUpdateApplied`. La replica originariamente contattata
  risponde al client con `WriteResult(true, idx, val, contactedReplicaId)`.
- `Client`: gestisce `ReadTimeout`/`WriteTimeout` con i delay forniti.

**Exit criteria**:
- `./gradlew test --tests "*APICompliance*oneClientWriteWaitRead*"` verde.
- `./gradlew test --tests "*APICompliance*callbackOnUpdateApplied*"` verde.
- `./gradlew test --tests "*NoCrashes*"` verde nei casi senza crash.

---

## Sprint 2 — Heartbeat + crash semplici

Obiettivo: il sistema sopravvive a crash isolati di repliche non-coordinatrici
e rileva la morte del coordinatore (senza ancora rieleggere).

Riferimento traccia §1 "Crash detection".

- `Heartbeat` periodico dal coordinatore ogni `getCoordinatorBeatInterval()`
  (default 1000 ms, regola 9 della codebase).
- Stato `CRASHED`: tramite `getContext().become(crashed())` con un
  `Receive` che droppa silenziosamente tutto (ma NON `stop`).
- Implementazione di `crash(Crash how_to_crash)` con contatori per
  `Crash.Type.{Now, Heartbeat, Update, WriteOK, Election}` — semantica
  "after_n_messages_of_type" = processa N, crasha al messaggio N+1
  (regola 10 codebase).
- `HeartbeatTimeout`: scatta dopo `coordinatorBeatInterval × k`
  (k ragionevole, ~3) — registra il crash del coordinatore ma NON parte
  ancora l'elezione (logging only). Il flag verrà collegato in Sprint 3.
- `ForwardTimeout` su write: il client riceve `WriteTimeout` se la replica
  contattata non riceve `WriteOk` entro la finestra.

**Exit criteria**:
- `APICompliance.replicasCrashNow` verde (tutte le repliche reagiscono al
  `Crash(Now, 0)`).
- `APICompliance.crashReplicaAndTryRequests` verde (client va in
  `ReadTimeout`/`WriteTimeout` quando la replica contattata è crashed).
- `WithCrashes` superato per i casi in cui crasha una replica
  non-coordinatrice durante un write (la maggioranza resta viva, l'update
  passa).

---

## Sprint 3 — Elezione + sincronizzazione

Obiettivo: dopo il crash del coordinatore, le repliche eleggono un nuovo
coordinatore via ring e completano gli update pendenti prima di accettarne
nuovi.

Riferimento traccia §1 "Coordinator election" + "Properties" (uniform
agreement).

- **Receive separato per ELECTION**: durante l'elezione la replica passa a
  un behavior dedicato che gestisce solo `Election`, `ElectionAck`,
  `Synchronization`, `Crash`, `ElectionAckTimeout`,
  `GlobalElectionTimeout`. Tutte le `Update`/`WriteOk` ricevute in questo
  stato vengono accodate o droppate secondo specifica.
- **Ring topology**: ordine crescente di `id`, il successore di `i` è
  `(i+1) mod N` skip-ando le repliche note come crashed.
- **Messaggio Election**: porta `Map<ReplicaId, UpdateID>` con il `latestId`
  noto a ciascuna replica (oppure lista di entries — scelta da motivare in
  report). Acked hop-by-hop con `ElectionAck` (traccia §1).
- **`ElectionAckTimeout`**: se il successore non ACKa, skip e forward al
  successivo (traccia §1: *"a replica that forwards an ELECTION message
  starts a timeout while waiting for the corresponding ACK"*).
- **Decisione del vincitore**: replica con `latestId` massimo; tie-break per
  `id` più alto (regola di codice).
- **`Synchronization`**: il vincitore broadcasta annuncio + lista degli
  `Update` mancanti per ciascuna replica (o lista compatta che le altre
  diffano contro la propria history).
- **Completamento update pendenti** prima di bumpare l'epoch: la traccia §1
  "Properties" lo richiede esplicitamente. Solo dopo aver "chiuso" gli
  update orfani il vincitore incrementa l'epoch via `nextEpoch()` e
  riprende le write.
- **`GlobalElectionTimeout`**: rete di sicurezza contro livelock — se
  l'elezione non termina entro N×ElectionAckTimeout, riparte.
- **Firing callback**: `callbackOnElectionStarted(crashedCoordId)` alla
  prima `Election` inviata; `callbackOnCoordinatorElected(newCoordId)` sul
  vincitore alla decisione e su ogni altra replica al processamento di
  `Synchronization` (regola 6 codebase + commenti nei test).

**Exit criteria**:
- `APICompliance.callbackOnElectionStartedInvokedCorrectly` verde per
  N ∈ {5,7}.
- `APICompliance.callbackOnElectionStartedCalledAtMostOncePerReplica`
  verde.
- `APICompliance.callbackOnCoordinatorElectedAllAgree` verde.
- `APICompliance.callbackOnCoordinatorElectedNewCoordAlsoCalls` verde.

---

## Sprint 4 — Corner case del fault model

Obiettivo: tutti i test del codebase (`NoCrashes`, `WithCrashes`,
`APICompliance`) verdi, inclusi gli scenari che la traccia §1 "Properties"
chiama "partial dissemination".

Casi minimi da coprire (traccia §2: *"trigger crashes at specific points in
the protocol execution"*):

1. Coordinatore crasha **durante il broadcast di UPDATE** (ne ha mandato
   ad alcuni, non a tutti).
2. Coordinatore crasha **dopo aver mandato WRITEOK ad alcuni** ma non a
   tutti — uniform agreement: il nuovo coord deve far convergere tutti.
3. **Due nodi consecutivi del ring** crashano durante l'elezione: il
   sender salta due hop.
4. Vincitore dell'elezione **crasha prima di mandare Synchronization** —
   parte una nuova elezione.
5. Replica crasha **dopo aver inviato ACK** ma prima di ricevere WriteOk
   (deve risultare trasparente per gli altri).
6. Client invia richiesta a replica crashed → `ReadTimeout`/`WriteTimeout`
   con i campi corretti.

**Exit criteria**: `./gradlew test` interamente verde, inclusi tutti i
`@ParameterizedTest` in `WithCrashes`.

---

## Sprint 5 — Report, demo e consegna

Riferimento traccia §3 (report) e §4 (presentation & submission).

### 5.1 Report LaTeX

- Template fornito (linkato in traccia §3) — già presente in `report/`.
- 3-4 pagine, **max 6** ("Reports exceeding this page limit will be
  automatically rejected").
- Inglese.
- Coprire le domande di esempio della slide `docs/ds1_project_2026_presentation.pdf`:
  scelte architetturali, gestione timeout, scelta della topologia del ring,
  trattamento degli update orfani, motivazioni di tie-break, assunzioni
  aggiuntive.
- Stato attuale `report/`: skeleton `main.tex` + `01_structure.tex`,
  `02_design.tex`, `03_implementation.tex` — da completare.

### 5.2 Demo scenarios

Traccia §4 raccomanda *"three or four representative execution examples,
including corner cases"*. Implementare in `Main.java` (o in classi
`demos/`) almeno:

- Demo 1 — happy path: 1 client, 3 write su 3 indici diversi, 1 read
  finale che verifica lo stato.
- Demo 2 — crash di una replica non-coord: il sistema continua a
  rispondere; la replica crashata risponde con timeout al client.
- Demo 3 — crash del coordinatore con elezione e sync: si vede il nuovo
  coord, le write successive vanno a buon fine.
- Demo 4 — crash del coord **durante** un UPDATE in corso: si vede
  l'update orfano completato dal nuovo coord (mostra l'uniform agreement).

Ogni demo deve produrre log puliti e leggibili (timestamp + pattern
ufficiali).

### 5.3 Checklist di consegna (traccia §4)

- [ ] Verificare `./gradlew test` interamente verde su clone pulito.
- [ ] Report in formato `.pdf` autocontenuto.
- [ ] Archivio `tar -czvf BianchiCognome2.tgz BianchiCognome2/` con dentro
  sorgenti + report (cartella con i due cognomi). **Non includere** le PDF
  del prof.
- [ ] Prenotare slot di presentazione via mail a Picco + Pasquali + Genetti
  prima della deadline del corrispondente slot.
- [ ] Indicare in-person vs online.
- [ ] 12 minuti di presentazione (timer rigido) + Q&A.

---

## Tracciabilità requisiti traccia → sprint

| Requisito traccia                                                | Sprint |
|------------------------------------------------------------------|--------|
| §1 Two-phase update (UPDATE/ACK/WRITEOK), quorum ⌊N/2⌋+1         | 1      |
| §1 UpdateID ⟨epoch, seq⟩                                          | 1      |
| §1 Heartbeat per liveness coordinatore                            | 2      |
| §1 Crash detection via timeout                                    | 2      |
| §1 Ring election, ACK hop-by-hop, skip su timeout                 | 3      |
| §1 Vincitore = max latest update, tie-break id                    | 3      |
| §1 Synchronization + completamento update pendenti                | 3,4    |
| §1 Uniform agreement con crash del coord a metà broadcast         | 4      |
| §2 Logging formattato e timestamped                               | 1+     |
| §2 Crashed mode (no `stop`)                                       | 2      |
| §2 Crash istrumentati per tipo di messaggio                       | 2      |
| §2 FIFO + latenza random emulata (NetworkChannel)                 | dato   |
| §2 Immutability dei messaggi inviati                              | 1+     |
| §3 Report 3-4 pagine LaTeX in inglese                             | 5      |
| §4 Demo 3-4 scenari + 12 min presentation                          | 5      |
