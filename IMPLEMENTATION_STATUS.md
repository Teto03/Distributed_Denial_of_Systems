# Stato implementativo

Documento di stato del progetto **Quorum-Based Total Order Broadcast**
(Distributed Systems 2025-2026). Descrive **cosa è stato implementato, dove
vive nel codice e perché funziona**, mettendolo a confronto riga per riga con
la traccia `docs/ds1_project_2026.pdf`. Identificatori, log e report restano in
inglese; la prosa è in italiano come da convenzione interna.

- Roadmap per sprint con criteri di uscita → [`ROADMAP.md`](ROADMAP.md)
- Contratto di interfaccia fra i due flussi di lavoro → [`CONTRACT_PHASE0.md`](CONTRACT_PHASE0.md)

**Ultimo aggiornamento**: 2026-08-17, su `main` (base `d15f887` + le modifiche
di oggi su `Replica.java` e `Main.java`, ancora non committate).
Tutti i branch remoti sono allineati: `sprint3-election` è già stato mergiato
in `main` (`6c01abe`) e non contiene lavoro ulteriore — `main` è l'unica linea
di sviluppo viva.

---

## Indice

1. [Sintesi: dove siamo](#1-sintesi-dove-siamo)
2. [Tracciabilità requisiti → codice](#2-tracciabilità-requisiti--codice)
3. [Layout del repository](#3-layout-del-repository)
4. [Infrastruttura fornita dalla codebase](#4-infrastruttura-fornita-dalla-codebase)
5. [Modello dati](#5-modello-dati)
6. [Catalogo dei messaggi](#6-catalogo-dei-messaggi)
7. [Percorso di una READ](#7-percorso-di-una-read)
8. [Percorso di una WRITE (two-phase)](#8-percorso-di-una-write-two-phase)
9. [Crash detection: heartbeat e timeout](#9-crash-detection-heartbeat-e-timeout)
10. [Crashed mode e istrumentazione dei crash](#10-crashed-mode-e-istrumentazione-dei-crash)
11. [Elezione del coordinatore](#11-elezione-del-coordinatore)
12. [Sincronizzazione e update orfani](#12-sincronizzazione-e-update-orfani)
13. [Come sono garantite le proprietà di safety](#13-come-sono-garantite-le-proprietà-di-safety)
14. [Tabella riassuntiva dei timer](#14-tabella-riassuntiva-dei-timer)
15. [Logging](#15-logging)
16. [Demo eseguibili](#16-demo-eseguibili)
17. [Test: cosa esiste e cosa misura](#17-test-cosa-esiste-e-cosa-misura)
18. [Cosa manca da fare](#18-cosa-manca-da-fare)
19. [Limitazioni note e assunzioni da dichiarare nel report](#19-limitazioni-note-e-assunzioni-da-dichiarare-nel-report)
20. [Come compilare ed eseguire](#20-come-compilare-ed-eseguire)

---

## 1. Sintesi: dove siamo

**Il protocollo è completo e funzionante.** Tutti i 93 test presenti nel
repository passano, inclusi quelli su crash del coordinatore, elezione e
sincronizzazione, e i quattro scenari di demo girano end-to-end.

| Sprint | Contenuto | Stato |
|--------|-----------|-------|
| 0 | Setup repo, Gradle wrapper, dipendenze | ✅ completato |
| 1 | Modello dati, messaggi, happy path client/replica | ✅ completato |
| 2 | Heartbeat, crashed mode, crash istrumentati, timeout | ✅ completato |
| 3 [B] | Logica pura di ring/vincitore/diff (`election/`) | ✅ completato |
| 3 [A+B] | Integrazione FSM: trigger, ack-skip, Synchronization, callback | ✅ completato |
| 4 | Corner case del fault model | ⚠️ **parziale** — tutti innescabili, test dedicati mancanti |
| 5.1 | Report LaTeX | ❌ **da fare** |
| 5.2 | Demo eseguibili | ✅ completato |
| 5.3 | Consegna (archivio, slot, presentazione) | ❌ da fare |

In sostanza: **manca il report e la copertura a test dei corner case**; il
cuore del sistema c'è tutto ed è dimostrabile dal vivo. Il dettaglio operativo
è in [§18](#18-cosa-manca-da-fare).

---

## 2. Tracciabilità requisiti → codice

Ogni riga è un requisito della traccia; la colonna "dove" indica il punto
esatto dell'implementazione.

| Requisito (traccia)                                                    | Dove                                                     | Stato |
|------------------------------------------------------------------------|----------------------------------------------------------|-------|
| §1 Ogni replica tiene una copia di `P[]`                                | `Replica.java:54` (`positions[POSITIONS_LIST_LENGTH]`)   | ✅ |
| §1 Read servita localmente dalla replica contattata                     | `Replica.java:331` `onClientRead`                        | ✅ |
| §1 Write inoltrata al coordinatore dalla replica contattata             | `Replica.java:360` `submitClientWrite`                   | ✅ |
| §1 UPDATE broadcast dal coordinatore                                    | `Replica.java:393` `startUpdate`                         | ✅ |
| §1 ACK da ogni replica                                                  | `Replica.java:402` `onUpdateMsg`                         | ✅ |
| §1 Quorum `⌊N/2⌋+1`, coordinatore incluso                               | `Replica.java:420` + `quorum()` `:764`                   | ✅ |
| §1 WRITEOK broadcast e applicazione locale                              | `Replica.java:434` `onWriteOk` → `:456` `applyUpdate`    | ✅ |
| §1 Id univoco `⟨e,i⟩`, epoch monotona, sequence azzerata a ogni epoch   | `UpdateID.java`, `Replica.java:394`, `:662`              | ✅ |
| §1 History degli update mantenuta per il recovery                       | `UpdateHistory.java`, `Replica.java:61`                  | ✅ |
| §1 Timeout su WRITEOK dopo aver ricevuto UPDATE                         | `Replica.java:838` `scheduleUpdateTimeout` + `:318`      | ✅ |
| §1 Timeout su write inoltrata (coordinatore non avvia il broadcast)     | `Replica.java:828` `scheduleForwardTimeout` + `:309`     | ✅ |
| §1 HEARTBEAT periodico dal coordinatore                                 | `Replica.java:262` `startHeartbeatLoop`                  | ✅ |
| §1 Ring definito dall'ordine degli id                                   | `election/RingTopology.java` `order`                     | ✅ |
| §1 ELECTION accumula l'update più recente noto a ciascuna replica       | `messages/Election.java` + `ElectionLogic.withEntry`     | ✅ |
| §1 "se non sta già partecipando, aggiunge la propria informazione"      | `ElectionLogic.withEntry` (`putIfAbsent`)                | ✅ |
| §1 ACK hop-by-hop dell'ELECTION                                         | `Replica.java:534` (ack immediato) + `:624`              | ✅ |
| §1 Timeout sull'ACK → skip del successore                               | `Replica.java:596` `onElectionAckTimeout`                | ✅ |
| §1 Vincitore = update più recente, tie-break su id                      | `ElectionLogic.winner`                                   | ✅ |
| §1 SYNCHRONIZATION per annunciare la leadership                         | `Replica.java:670`                                       | ✅ |
| §1 Il nuovo coordinatore allinea gli altri prima di riprendere le write | `Replica.java:647` `becomeWinner` (ordine delle fasi)    | ✅ |
| §1 Completamento degli update interrotti (partial dissemination)        | `Replica.java:690` `completeInterruptedUpdates`          | ✅ |
| §2 Akka, attori, Java                                                   | tutto il progetto                                        | ✅ |
| §2 Uso obbligatorio della codebase, `createBaseReceiveBuilder()`        | `Replica.java:210`, `:246`; `Client.java:67`             | ✅ |
| §2 Canali FIFO con latenza random                                       | `NetworkChannel.java`, usato via `tell()`                | ✅ |
| §2 Nessuno stato mutabile condiviso; messaggi immutabili                | `messages/*` tutti `final`+`Serializable`+copie difensive| ✅ |
| §2 Crashed mode senza `stop()`                                          | `Replica.java:181` `crashed()`                           | ✅ |
| §2 Crash istrumentati per tipo di messaggio                             | `Replica.java:870` `checkCrashCondition`                 | ✅ |
| §2 Crash **a metà** del broadcast di UPDATE / WRITEOK                   | `Replica.java:785` `broadcast(msg, crashPoint)`          | ✅ |
| §2 Log con timestamp e formato prescritto                               | `Logger.java` + `log()` in tutta `Replica`               | ✅ |
| §2 Esecuzione di sequenze di write interlacciate a crash                | `Main.java` (demo 2-4), `WithCrashes`                    | ✅ |
| §3 Report 3-4 pagine in inglese                                         | `report/`                                                | ❌ vuoto |
| §4 3-4 scenari di esecuzione rappresentativi                            | `Main.java`                                              | ✅ |

---

## 3. Layout del repository

```
SD_PROJECT/
├── build.gradle                # Akka classic 2.6.13 + JUnit 6, plugin application
├── gradlew, gradle/            # wrapper Gradle 9.2.1
├── README.md
├── ROADMAP.md                  # piano per sprint
├── CONTRACT_PHASE0.md          # contratto delle interfacce fra i due flussi
├── IMPLEMENTATION_STATUS.md    # questo documento
├── docs/                       # traccia, slide, guida di pianificazione interna
├── report/                     # skeleton LaTeX (main.tex compila, sezioni vuote)
└── src/
    ├── main/java/it/unitn/ds/
    │   ├── AbstractClient.java     # CODEBASE — non modificare
    │   ├── AbstractReplica.java    # CODEBASE — non modificare
    │   ├── Logger.java             # CODEBASE — logging timestamped
    │   ├── NetworkChannel.java     # CODEBASE — canale FIFO con ritardo random
    │   ├── Main.java               # NOSTRO — i quattro scenari di demo
    │   ├── Client.java             # NOSTRO — 140 righe
    │   ├── Replica.java            # NOSTRO — il cuore del progetto
    │   ├── UpdateID.java           # NOSTRO — ⟨epoch, sequence⟩
    │   ├── Update.java             # NOSTRO — entry della history
    │   ├── UpdateHistory.java      # NOSTRO — log append-only
    │   ├── election/               # NOSTRO — logica pura, senza attori
    │   │   ├── RingTopology.java
    │   │   ├── ElectionLogic.java
    │   │   └── SyncPlan.java
    │   └── messages/               # NOSTRO — 17 POJO immutabili
    └── test/java/it/unitn/ds/
        ├── TestsCommons.java       # CODEBASE — helper e costanti di timing
        ├── base/                   # CODEBASE — test obbligatori
        │   ├── NoCrashes.java      (4 casi)
        │   ├── WithCrashes.java    (4 casi)
        │   └── APICompliance.java  (25 casi)
        └── election/               # NOSTRO — unit test puri (60 casi)
```

---

## 4. Infrastruttura fornita dalla codebase

Vincoli che l'implementazione deve rispettare e su cui si appoggia.

**`AbstractReplica`** (`AbstractReplica.java`)
- Costanti: `MIN_LATENCY=5`, `MAX_LATENCY=20`, `COORDINATOR_BEAT_INTERVAL=1000`,
  `POSITIONS_LIST_LENGTH=100`.
- `tell(Serializable, ActorRef)` (`:96`) — **l'unico modo lecito di inviare**.
  Crea pigramente un attore `NetworkChannel` per ogni destinazione, figlio del
  mittente, e ci accoda il messaggio. Da qui derivano due proprietà usate in
  tutto il protocollo: **FIFO per coppia (mittente, destinatario)** e **ritardo
  casuale in `[5,20) ms`** su ogni hop.
- `getMaxLatencyPlusTolerance()` (`:88`) = `maxLatency + maxLatency/2 × N` — è
  la stima "un hop più tolleranza proporzionale alla taglia del sistema", usata
  per dimensionare l'`ElectionAckTimeout`.
- `createBaseReceiveBuilder()` (`:385`) — aggancia `Crash` (sempre) e
  `InitSystem` (finché non inizializzata). **Ogni `Receive` della replica deve
  partire da qui**, altrimenti i crash non arrivano più: è il motivo per cui
  anche `election()` (`Replica.java:246`) parte da questo builder e non da
  `receiveBuilder()`.
- Callback obbligatorie `final` (non sovrascrivibili): `callbackOnUpdateApplied`,
  `callbackOnElectionStarted`, `callbackOnCoordinatorElected`. Loggano e
  notificano il `listener` (la `TestKit` probe nei test).
- `onCrashMsg` (`:370`) chiama `crash(...)` **e poi** logga `CRASHED` e notifica
  il listener: il log `CRASHED: WriteOK (2)` compare quindi alla *ricezione* del
  comando di crash, non quando la replica muore davvero. Nei log della Demo 4 si
  vede infatti la riga `CRASHED` all'inizio e la morte effettiva tre secondi
  dopo, a metà del broadcast del WRITEOK.

**`AbstractClient`** — espone `ReadRequest`/`WriteRequest` (i messaggi che il
*test harness* manda al client), i tipi risultato `ReadResult`/`WriteResult`/
`ReadTimeout`/`WriteTimeout` e le quattro callback obbligatorie. Da qui la
scelta di §6: servono messaggi **nostri** per il dialogo client ↔ replica.

**`NetworkChannel`** — un attore per destinazione, coda FIFO, consegna il
prossimo elemento dopo un ritardo casuale. Nota importante per il fault model:
i canali sono **figli del mittente e non vengono fermati dal crash**, quindi
i messaggi già accodati prima del crash vengono comunque recapitati (vedi §19).

---

## 5. Modello dati

### `UpdateID` — `⟨epoch, sequence⟩`
Immutabile, `Serializable`, `Comparable` con ordine lessicografico (prima
`epoch`, poi `sequence`): un epoch più alto batte qualunque sequence. Helper
`nextInEpoch()` (usato dal coordinatore a ogni proposta) e `nextEpoch()`.
`toString()` → `<e,i>`.
Traccia §1: *"Each update is uniquely identified by a pair ⟨e,i⟩ ... Epoch
numbers increase monotonically ... The sequence number is reset to 0 at the
beginning of each epoch"*.

### `Update` — `(UpdateID id, int index, int value)`
Entry *deliverata* nella history. Nessun `ActorRef` dentro: è la
rappresentazione persistente e serializzabile del log, indipendente da chi
l'ha applicata (i dati di routing viaggiano in `UpdateMsg`, non qui). Questo
permette di rispedirla tale e quale dentro `Synchronization`.

### `UpdateHistory` — log append-only
Solo `append()`, mai rimozioni o riordini. Metodi usati dal protocollo:
- `latestId()` → `Optional<UpdateID>`: input della scelta del vincitore;
- `after(threshold)` → entries strettamente più recenti, in ordine: base del
  diff di sincronizzazione;
- `asList()` → snapshot `unmodifiable` per la serializzazione.

---

## 6. Catalogo dei messaggi

17 classi in `messages/`, tutte `final`, `Serializable`, campi `public final`,
nessun setter, collezioni copiate difensivamente e wrappate `unmodifiable*`
(traccia §2: *"any shared objects must be immutable"*).

> **Nota di nomenclatura.** Il broadcast di fase 1 si chiama `UpdateMsg` e non
> `Update` per non collidere con la data class della history. `UpdateMsg`
> *wrappa* un `Update` immutabile, così la stessa istanza può essere appesa
> alla history senza riallocazioni.

### Client ↔ replica (nostri)

| Classe        | Direzione        | Campi                                                     |
|---------------|------------------|-----------------------------------------------------------|
| `ClientRead`  | client → replica | `long reqId`, `int index`                                 |
| `ClientWrite` | client → replica | `long reqId`, `int index`, `int value`                    |
| `ReadReply`   | replica → client | `reqId`, `index`, `value`, `int fromReplica`              |
| `WriteReply`  | replica → client | `reqId`, `index`, `value`, `int fromReplica`              |

Il `reqId` è un contatore locale al client (`Client.java:28`) e serve a
**accoppiare risposta e timeout alla richiesta che li ha generati** quando ci
sono più richieste in volo. Per questo viaggia anche dentro `ForwardWrite` e
`UpdateMsg`: quando il `WriteOk` torna indietro, la replica contattata sa a chi
rispondere e con quale id.

### Protocollo two-phase

| Classe          | Direzione                | Campi                                                      | Note |
|-----------------|--------------------------|-------------------------------------------------------------|------|
| `ForwardWrite`  | replica → coordinatore   | `index`, `value`, `ActorRef client`, `contactedReplicaId`, `reqId` | `contactedReplicaId` è indispensabile: il `WriteResult` finale deve avere `fromReplica` = replica *contattata dal client*, non il coordinatore. |
| `UpdateMsg`     | coordinatore → repliche  | `Update update`, `ActorRef client`, `contactedReplicaId`, `reqId` | Fase 1. |
| `UpdateAck`     | replica → coordinatore   | `UpdateID id`                                              | Fase 1, risposta. |
| `WriteOk`       | coordinatore → repliche  | `UpdateID id`                                              | Fase 2. |

### Liveness ed elezione

| Classe            | Direzione                | Campi                                                        |
|-------------------|--------------------------|--------------------------------------------------------------|
| `Heartbeat`       | coordinatore → repliche  | — |
| `Election`        | replica → replica (ring) | `int initiatorId`, `Map<Integer,UpdateID> latestPerReplica` (copia difensiva + `unmodifiableMap`) |
| `ElectionAck`     | replica → replica        | — (decisione D1: ack vuoto) |
| `Synchronization` | nuovo coord → repliche   | `int newCoordinatorId`, `int newEpoch`, `List<Update> pendingUpdates` (copia + `unmodifiableList`) |

### Timer self-schedulati

| Classe                    | Chi lo arma e quando |
|---------------------------|----------------------|
| `HeartbeatTimeout`        | ogni non-coordinatore, riarmato a ogni `Heartbeat`/`UpdateMsg`/`WriteOk` ricevuto |
| `ForwardTimeout(reqId, index, value)` | replica che ha inoltrato una write e non vede partire il broadcast |
| `UpdateTimeout(UpdateID)` | replica che ha ack-ato la fase 1 e non vede il `WriteOk` |
| `ElectionAckTimeout(successorId)` | mittente di una `Election` in attesa dell'ack del successore |
| `GlobalElectionTimeout`   | rete anti-livelock, armata all'ingresso in ELECTION |

---

## 7. Percorso di una READ

Traccia §1: *"The contacted replica immediately replies with the current value
stored at that index"*.

1. `Client.sendRead` (`Client.java:48`) genera un `reqId`, lo mette in
   `pending`, logga `requesting READ (idx) to Replica_k`, invia `ClientRead` e
   arma un tick di timeout su sé stesso.
2. `Replica.onClientRead` (`Replica.java:331`) legge `positions[idx]` e
   risponde `ReadReply(reqId, idx, value, id)` — `fromReplica` è **la replica
   contattata**.
3. `Client.onReadReply` (`Client.java:75`) rimuove il `reqId` da `pending` e,
   solo se era ancora pendente, invoca `callbackOnReadResult`.
4. Se il tick scatta prima della risposta, `onReadTick` trova il `reqId` ancora
   pendente e invoca `callbackOnReadTimeout`.

**Scelta implementativa**: nessun `Cancellable` lato client. Il tick è sempre
schedulato e la mutua esclusione fra "risposta" e "timeout" è realizzata dalla
semantica di `Set.remove` — vince chi arriva primo, un tick tardivo è
silenziosamente ignorato. Meno stato, nessuna race.

Le read sono servite **anche durante un'elezione** (`Replica.java:247`): non
coinvolgono il coordinatore, quindi non c'è motivo di bloccarle. La
conseguenza è che una read durante l'elezione può restituire un valore che
non include un update ancora in volo — coerente con la consistenza sequenziale
richiesta dalla traccia (§1: *"sequential consistency from each replica's point
of view"*), non con la linearizzabilità.

---

## 8. Percorso di una WRITE (two-phase)

### Fase 0 — arrivo e inoltro
`onClientWrite` (`:347`) registra la richiesta in `clientWrites` (una
`LinkedHashMap` per `reqId`, così l'ordine FIFO per client sopravvive a un
eventuale replay) e chiama `submitClientWrite` (`:360`):
- se siamo il coordinatore → `startUpdate` diretto;
- altrimenti → `ForwardWrite` al coordinatore **e** `ForwardTimeout` armato.

### Fase 1 — proposta
`startUpdate` (`:393`) incrementa `lastAssignedId` con `nextInEpoch()`, crea
l'`Update`, azzera il contatore di ack e fa
`broadcast(UpdateMsg, Crash.Type.Update)` — il broadcast include **il
coordinatore stesso**, come richiede la traccia (*"Since the coordinator itself
is also a replica, the quorum includes the coordinator"*).

`onUpdateMsg` (`:402`), su ogni replica:
1. `checkCrashCondition(Update)` — punto di crash istrumentato;
2. riarma l'`HeartbeatTimeout` (l'`UpdateMsg` è di per sé prova di vita del
   coordinatore, non serve aspettare il prossimo heartbeat);
3. cancella il `ForwardTimeout` di quel `reqId` — il coordinatore *ha* avviato
   il broadcast;
4. memorizza la proposta in `pendingUpdates` (mappa `UpdateID → UpdateMsg`);
5. invia `UpdateAck` e arma l'`UpdateTimeout` per quell'id.

### Fase 2 — commit
`onUpdateAck` (`:420`) è ignorato da chi non è coordinatore. Il coordinatore
accumula in `ackCounts`; al raggiungimento di `quorum() = N/2 + 1` marca l'id
in `committed` (idempotenza: gli ack successivi non rifanno il broadcast) e
fa `broadcast(WriteOk, Crash.Type.WriteOK)`.

`onWriteOk` (`:434`) su ogni replica: crash check, riarmo dell'heartbeat
timeout, cancellazione dell'`UpdateTimeout`, e se la proposta è ancora in
`pendingUpdates` → `applyUpdate`.

`applyUpdate` (`:456`) è **l'unico punto in cui lo stato cambia**:
```java
positions[u.index] = u.value;
history.append(u);
log("applied update " + epoch + ":" + sequence + " (" + index + ", " + value + ")");
callbackOnUpdateApplied(u.index, u.value);
```
poi rimuove la proposta da `pendingUpdates` (garantendo che una seconda
consegna dello stesso id non riapplichi nulla) e, **solo se
`contactedReplicaId == id`**, risponde al client con `WriteReply` e libera la
`clientWrites`. È così che `WriteResult.fromReplica` risulta uguale alla
replica contattata, come pretendono i test della codebase.

Il formato di log è esattamente quello della traccia §2:
`[Replica <id>] applied update <epoch>:<sequence> (<idx>, <val>)`.

---

## 9. Crash detection: heartbeat e timeout

Traccia §1 "Crash detection" richiede tre rilevatori distinti; ci sono tutti.

**Heartbeat del coordinatore** — `startHeartbeatLoop` (`:262`) usa
`scheduleWithFixedDelay(Zero, coordinatorBeatInterval)` e si manda un
`SendHeartbeatTick` (classe interna, `:117`), che nel `Receive` (`:217`) si
traduce in `broadcast(new Heartbeat(), Crash.Type.Heartbeat)`. Passare per un
self-message invece di inviare direttamente dallo scheduler tiene tutti gli
invii dentro il thread dell'attore.

**`HeartbeatTimeout`** — `resetHeartbeatTimeout` (`:281`) arma un one-shot a
`3 × coordinatorBeatInterval` (3 s con i default). È **riarmato da tre eventi**:
`Heartbeat`, `UpdateMsg`, `WriteOk` — qualsiasi prova di vita del coordinatore
vale. Alla scadenza (`:302`) → `startElection(coordinatorId)`.
Il fattore 3 è largamente sopra il RTT massimo (`maxLatency = 20 ms`), quindi
rispetta il requisito *"Crash detection is assumed to be accurate and does not
produce false positives"*.

**`ForwardTimeout`** — armato in `scheduleForwardTimeout` (`:828`) a
`3 × coordinatorBeatInterval` quando una replica inoltra una write. Copre il
caso della traccia *"a replica that forwards a write request to the
coordinator starts a timeout and detects a failure if the coordinator does not
initiate the broadcast protocol in time"*. È **chiavato per `reqId`** e non per
`(index, value)`: due richieste diverse possono scrivere lo stesso valore
nello stesso indice (retry del client), quindi la coppia non sarebbe una
chiave sicura. Alla scadenza → elezione.

**`UpdateTimeout`** — armato in `scheduleUpdateTimeout` (`:838`) subito dopo
l'invio dell'`UpdateAck`, cancellato dal `WriteOk` corrispondente. Copre
*"A replica detects that the coordinator has crashed if it does not receive a
WRITEOK message within a predefined timeout after receiving an UPDATE"*. Alla
scadenza → elezione.

Entrambi gli scheduler helper (`:828`, `:838`) **cancellano il timer precedente
prima di riarmare**: riassegnare la variabile senza cancellare lascerebbe il
vecchio timer vivo e produrrebbe timeout spuri.

---

## 10. Crashed mode e istrumentazione dei crash

**Modalità crashed** — `triggerCrash()` (`:176`) cancella *tutti* i timer
(`cancelAllTimers`, `:803`) e fa `become(crashed())`. Il behavior `crashed()`
(`:181`) è un `matchAny` che scarta silenziosamente qualunque messaggio, con
commento esplicito sul fatto che **non** si chiama `getContext().stop()`, come
impone la traccia §2. Cancellare i timer è ciò che realizza il *"stops sending
messages to other actors"*: senza questo, una replica "morta" continuerebbe a
emettere heartbeat o a far scattare elezioni.

**`Crash.Type.Now`** — `crash(...)` (`:162`) crasha immediatamente, **ignorando
`after_n_messages_of_type`**: per `Now` non esiste un tipo di messaggio da
contare, e trattare un `n > 0` come una condizione differita significherebbe
armare un contatore che nessuno incrementa mai, lasciando viva una replica che
il chiamante crede morta.

**Crash differiti** — gli altri quattro tipi sono memorizzati in
`pendingCrash` e valutati da `checkCrashCondition(type)` (`:870`), che
implementa la semantica *"processa N messaggi di quel tipo, poi crasha"*:

```java
if (pendingCrash != null && pendingCrash.type == type) {
    int currentCount = messageCounters.getOrDefault(type, 0);
    if (currentCount >= pendingCrash.after_n_messages_of_type) { triggerCrash(); return false; }
    messageCounters.put(type, currentCount + 1);
}
return true;
```

Con `after_n = 2`: il primo e il secondo messaggio vengono processati
(contatore 0→1→2), al terzo `2 >= 2` e la replica crasha **senza** processarlo.
Con `after_n = 0` crasha sul primo.

**I due lati del contatore.** La condizione è valutata in due posti diversi, e
questo è ciò che copre tutti i punti di crash chiesti dalla traccia §2
(*"during the broadcast of an UPDATE, after receiving an UPDATE, during the
dissemination of WRITEOK messages, or while the coordinator election is in
progress"*):

- **in ricezione**, all'inizio degli handler di `UpdateMsg` (`:404`),
  `WriteOk` (`:436`), `Heartbeat` (`:294`) ed `Election` (`:530`);
- **in invio**, dentro `broadcast(msg, crashPoint)` (`:785`), che valuta la
  condizione **una volta per destinatario**:

```java
private void broadcast(Serializable msg, AbstractReplica.Crash.Type crashPoint) {
    for (int replicaId : RingTopology.order(group.keySet())) {
        if (!checkCrashCondition(crashPoint)) {
            log("crashed while broadcasting " + msg + ": the remaining replicas will not receive it");
            return;
        }
        tell(msg, group.get(replicaId));
    }
}
```

Il contatore è **uno solo** e non c'è ambiguità, perché per un dato tipo una
replica o è quella che lo broadcasta (il coordinatore) o è una delle
destinatarie, mai entrambe. Quindi:

| Comando | Sul coordinatore | Su una non-coordinatrice |
|---------|------------------|--------------------------|
| `Crash(Update, n)` | muore dopo aver inviato l'UPDATE a `n` repliche | muore dopo aver processato `n` UPDATE |
| `Crash(WriteOK, n)` | muore dopo aver inviato il WRITEOK a `n` repliche | muore dopo `n` WRITEOK |
| `Crash(Heartbeat, n)` | muore dopo `n` heartbeat inviati | muore dopo `n` heartbeat ricevuti |
| `Crash(Election, n)` | — | muore dopo `n` messaggi ELECTION processati |
| `Crash(Now, 0)` | muore subito | muore subito |

**È questa la parte che rende dimostrabile la "partial dissemination"**: con
`Crash(WriteOK, 2)` il coordinatore consegna il WRITEOK a due repliche soltanto
e muore, ed è esattamente lo scenario della Demo 4 (§16).

Due dettagli deliberati:

- il ciclo scorre le repliche in **ordine crescente di id**
  (`RingTopology.order`) e non sui `values()` della `HashMap`: così l'insieme
  delle repliche servite prima del crash è riproducibile fra un'esecuzione e
  l'altra, che è quello che serve a una demo;
- la `Synchronization` **non** passa da `broadcast` (è un ciclo a parte in
  `becomeWinner`, perché deve escludere il vincitore): il crash "durante
  l'elezione" resta quello sul messaggio `Election`, che è il punto citato
  dalla traccia.

---

## 11. Elezione del coordinatore

### Logica pura, isolata dagli attori — package `election/`

Tre classi `final` con soli metodi `static`, senza stato: testabili senza
`ActorSystem`, deterministiche, e riutilizzabili come "seam" fra la FSM e i
dati del protocollo.

**`RingTopology`**
- `order(memberIds)` → lista crescente `unmodifiable`, costruita passando da un
  `TreeSet` (ordina *e* deduplica in un colpo solo, così `group.keySet()` e una
  lista arbitraria producono lo stesso ring).
- `successor(self, memberIds, suspected)` → primo id non sospettato nella
  sequenza di hop. **Non usa aritmetica modulare su indici** (fragile con id
  non contigui) ma costruisce esplicitamente la sequenza: prima gli id maggiori
  di `self` in ordine, poi quelli minori (wrap-around). Conseguenze volute:
  `self` è escluso per costruzione (una replica non è mai successore di sé
  stessa), si saltano **N sospettati consecutivi** e non uno solo, lo skip
  funziona anche attraverso il wrap-around, e se tutti gli altri sono
  sospettati si ottiene `Optional.empty()` invece di un loop su sé stessi.

**`ElectionLogic`**
- `NONE = <0,0>` — sentinella per history vuota (decisione D2), strettamente
  minore di qualunque id reale (il primo update di un epoch è `<e,1>`).
- `winner(latestPerReplica)` — massimo `UpdateID`, tie-break sull'**id più
  alto**. Funzione pura del solo payload: ogni replica che vede il giro
  completo calcola lo stesso vincitore, senza round aggiuntivi.
- `latestOf(history)` — `latestId()` oppure `NONE`; rende impossibile mettere
  `null` nel payload.
- `withEntry(map, id, latest)` — copia + `putIfAbsent`. Il "first writer wins"
  traduce alla lettera *"if it is not already participating in the election, it
  adds its own information"*: se rivedo il mio id il messaggio ha completato il
  giro e il payload deve restare **identico** a quello visto dagli altri,
  altrimenti repliche diverse potrebbero eleggere vincitori diversi.
- `newEpoch(latestPerReplica)` = `max(epoch visti) + 1` (decisione D3). Il
  massimo è preso su **tutto il payload** e non solo sulla history del
  vincitore: se una replica ha visto un epoch più alto, riusare un epoch già
  speso romperebbe l'unicità degli `UpdateID`.

**`SyncPlan`** — vedi §12.

### La FSM dentro `Replica`

Due behavior, entrambi costruiti su `createBaseReceiveBuilder()`:

| Behavior | Costruito in | Accetta |
|----------|--------------|---------|
| NORMAL | `createReceive()` `:209` | tutto il protocollo, **inclusi** `Election`/`ElectionAck`/`Synchronization` — una replica che non ha ancora rilevato il crash può essere trascinata in un round o apprenderne l'esito restando in NORMAL |
| ELECTION | `election()` `:245` | `ClientRead`, `ClientWrite` (bufferizzata), i quattro messaggi di elezione, `Synchronization`, `Crash`; **tutto il resto è scartato** con un `debug(...)` |
| CRASHED | `crashed()` `:181` | niente |

Scartare `UpdateMsg`/`WriteOk`/`Heartbeat` durante ELECTION è la traduzione
dell'Hint 1 della guida interna: **congela il "latest known update" di ogni
candidato per tutta la durata del round**, così il payload dell'`Election` non
può diventare inconsistente mentre gira.

### Stato di elezione mantenuto (`Replica.java:100-117`)

| Campo | Ruolo |
|-------|-------|
| `Set<Integer> suspected` | repliche note come morte: il coordinatore crashato più ogni successore che non ha ack-ato. Cresce soltanto (le repliche non si riprendono). |
| `boolean participating` | vero mentre siamo nel behavior ELECTION |
| `boolean electionStartedFired` | garantisce **al massimo una** `callbackOnElectionStarted` per elezione, anche se il round viene riavviato dal `GlobalElectionTimeout` |
| `Election lastForwardedElection` | ultimo payload inoltrato, da rigiocare quando si salta un successore |

### Avvio — `startElection` (`:485`)

1. guardia di idempotenza (`participating`);
2. `suspected.add(crashedCoordinatorId)`;
3. **cancella l'`HeartbeatTimeout`**: mentre siamo candidati non dobbiamo
   sospettare nessun altro per assenza di heartbeat;
4. `become(election())`, `fireElectionStartedOnce(...)`, arma il
   `GlobalElectionTimeout`;
5. costruisce il payload con la sola entry propria e lo inoltra al successore.

### Circolazione — `onElection` (`:528`)

1. `checkCrashCondition(Election)` — punto di crash "durante l'elezione";
2. **ack immediato al mittente** (`:534`), prima di qualunque altra logica: chi
   ha inoltrato deve poter cancellare il proprio `ElectionAckTimeout` al più
   presto;
3. `isStaleElection` (`:583`) — scarta un `Election` ritardatario di un round
   già deciso, cioè uno che eleggerebbe il coordinatore che stiamo già
   seguendo; se siamo noi il coordinatore rispondiamo con una
   `Synchronization` mirata, così il mittente rimasto indietro può uscire dal
   round invece di restarci bloccato;
4. se non stiamo già partecipando → entriamo in ELECTION (stesso setup di
   `startElection`, con `callbackOnElectionStarted(coordinatorId)`: il
   coordinatore in cui credevamo è quello crashato);
5. **se il payload contiene già il nostro id** → il giro è completo, il payload
   è definitivo: calcoliamo `winner(...)`. Se siamo noi → `becomeWinner`;
   altrimenti inoltriamo così com'è, perché il messaggio deve **raggiungere il
   vincitore**, unico autorizzato ad annunciare l'esito;
6. altrimenti → inoltriamo `withEntry(payload, id, latestOf(history))`.

### Skip del successore silenzioso

`rearmElectionAckTimeout` (`:624`) arma `ElectionAckTimeout(successorId)` a
`getMaxLatencyPlusTolerance()`. `onElectionAck` (`:591`) lo cancella.
`onElectionAckTimeout` (`:596`) aggiunge il successore a `suspected`, logga e
**rigioca `lastForwardedElection`** verso il successore successivo — che
`RingTopology.successor` calcola già saltando tutti i sospettati. Traccia §1:
*"the sender assumes that the next replica in the ring has crashed, skips it,
and forwards the message to the following replica"*.

Se `successor` restituisce `Optional.empty()` (siamo gli unici vivi),
`forwardElection` (`:514`) vince per default.

### Anti-livelock

`rearmGlobalElectionTimeout` (`:629`) arma un one-shot a
`N × getMaxLatencyPlusTolerance() × 2` — abbastanza per un giro completo del
ring. Alla scadenza (`:613`) il round riparte da zero **senza rifare la
callback** (`electionStartedFired` resta `true`): è ancora la stessa
partecipazione, solo un altro tentativo. È la rete di sicurezza per il caso in
cui il vincitore crashi prima di annunciarsi (corner case 4 dello Sprint 4):
al riavvio, il primo tentativo di inoltro verso il vincitore morto scade
sull'`ElectionAckTimeout`, che lo aggiunge a `suspected`, e il round prosegue
senza di lui.

---

## 12. Sincronizzazione e update orfani

### `SyncPlan` — il diff

- `missingFor(winnerHistory, recipientLatest)` — wrapper su
  `UpdateHistory.after(...)`, tenuto come seam esplicito perché il livello di
  elezione non tocchi mai gli interni della history. Totale anche nei casi
  degeneri (destinatario più avanti del vincitore → lista vuota).
- `oldest(latestPerReplica)` — minimo dei `latestId` del payload.
- `missingForAll(winnerHistory, latestPerReplica)` = `missingFor(history, oldest(...))`.

Il motivo di `missingForAll` è strutturale: `Synchronization` è un **broadcast
unico** con una sola lista, quindi il vincitore non può spedire una lista
personalizzata per destinatario. La lista giusta è allora il diff rispetto
alla **replica più indietro** fra i partecipanti; chi ha già applicato una
parte di quegli update li scarta, perché la consegna è **idempotente
sull'`UpdateID`** (property Integrity: *"a process delivers m at most once"*).

### `becomeWinner` (`:647`) — l'ordine delle fasi è la parte critica

```
1. cancella ElectionAckTimeout e GlobalElectionTimeout, azzera lo stato di elezione
2. newEpoch = ElectionLogic.newEpoch(payload)
3. completeInterruptedUpdates()          ← PRIMA di aprire il nuovo epoch
4. coordinatorId = id;  lastAssignedId = <newEpoch, 0>;  azzera ackCounts/committed
5. missing = SyncPlan.missingForAll(history, payload)   ← include ciò che ha appena applicato
6. Synchronization(id, newEpoch, missing) a tutti tranne sé stesso
7. become(createReceive()); startHeartbeatLoop()
8. callbackOnCoordinatorElected(id)
9. replayPendingClientWrites()
```

Il punto 3 **deve** precedere il 5: gli update interrotti dell'epoch morto
vengono prima deliverati localmente e finiscono quindi dentro la
`Synchronization`. È esattamente ciò che la traccia §1 "Properties" chiede:
*"a newly elected coordinator must detect and complete any update broadcasts
that were interrupted by the previous coordinator's failure"*.

`completeInterruptedUpdates` (`:690`) itera i `pendingUpdates` **ordinati per
`UpdateID`** (`Collections.sort` sulle chiavi) — l'ordine totale va preservato
anche in recovery — salta quelli già deliverati e applica gli altri.

### `onSynchronization` (`:707`) — lato ricevente

Prima di tutto una **guardia di idempotenza**: se non stiamo partecipando a
un'elezione e l'annuncio riguarda il coordinatore e l'epoch che abbiamo già
adottato, il messaggio è un duplicato e viene ignorato. Serve davvero: il nuovo
coordinatore risponde con una `Synchronization` a **ogni** `Election`
ritardataria che gli arriva (vedi `isStaleElection`), quindi la stessa replica
può ricevere lo stesso annuncio tre o quattro volte. Rieseguire l'handler
rigiocherebbe le write bufferizzate una seconda volta, trasformando **una**
richiesta del client in **due** update distinti.

Superata la guardia: cancella i timer di elezione, azzera lo stato, adotta il
nuovo `coordinatorId`, poi

- **replay filtrato**: `if (!alreadyDelivered(u.id)) applyUpdate(u)` — il
  filtro di idempotenza è ciò che impedisce una seconda
  `callbackOnUpdateApplied` per la stessa write;
- scarta tutto ciò che resta dell'epoch morto (`pendingUpdates`, `ackCounts`,
  `committed`, `updateTimeouts`): da qui in poi la sola sorgente di verità è
  il nuovo coordinatore;
- `lastAssignedId = <newEpoch, 0>`;
- torna in NORMAL, riarma l'`HeartbeatTimeout`, chiama
  `callbackOnCoordinatorElected` e rigioca le write bufferizzate.

`alreadyDelivered(uid)` (`:473`) confronta con `history.latestId()`: la history
è ordinata, quindi "id ≤ ultimo" significa "già visto".

### Write che sopravvivono all'elezione

Il client emette ogni richiesta **una sola volta** e non ritenta: una write il
cui coordinatore muore a metà andrebbe persa per sempre. Perciò:
- `clientWrites` (`:90`, `LinkedHashMap` per preservare l'ordine FIFO del
  client) trattiene ogni write per cui questa replica è stata contattata e non
  ha ancora risposto;
- durante ELECTION le nuove write sono **parcheggiate**, non scartate
  (`onClientWriteDuringElection`, `:354`) — decisione D4;
- `replayPendingClientWrites` (`:375`) le rimanda al nuovo coordinatore appena
  l'elezione si chiude, sia sul vincitore sia su chi riceve la
  `Synchronization`;
- l'entry è rimossa solo in `applyUpdate`, quando il client riceve davvero il
  suo `WriteReply`.

---

## 13. Come sono garantite le proprietà di safety

Le quattro proprietà del riquadro "Background: total order broadcast" della
traccia:

**Validity** — *se il mittente è corretto, prima o poi consegna m*. Una write
di un client corretto raggiunge il coordinatore; se il coordinatore muore
prima del commit, `ForwardTimeout`/`UpdateTimeout`/`HeartbeatTimeout`
producono un'elezione e `replayPendingClientWrites` rimette la richiesta in
circolo verso il nuovo coordinatore.

**Integrity** — *consegna al più una volta*. Tre filtri indipendenti:
`pendingUpdates.remove` in `applyUpdate` (fase 2 normale), il check
`alreadyDelivered` nel replay della `Synchronization`, e lo stesso check in
`completeInterruptedUpdates`. `callbackOnUpdateApplied` è invocata esattamente
in un punto del codice (`:461`), dentro `applyUpdate`. Sul versante della
richiesta del client, la guardia sui duplicati di `onSynchronization` impedisce
che una sola write venga rigiocata due volte verso il nuovo coordinatore.

**Uniform Agreement** — *se una replica consegna m, tutte le repliche corrette
consegnano m*. È il caso critico "coordinatore morto dopo aver mandato WRITEOK
solo ad alcuni", quello che la Demo 4 riproduce. Chi ha applicato l'update ha
`latestId ≥ <e,i>`; il vincitore dell'elezione è per costruzione la replica con
il `latestId` massimo, quindi **ha necessariamente quell'update in history**
(assunzione della traccia: un quorum resta sempre vivo, e almeno una replica
corretta conosce l'update più recente). `missingForAll` lo include nel
broadcast, e ogni replica indietro lo applica. Il ramo simmetrico — il
coordinatore muore dopo aver ricevuto il quorum di ack ma prima di mandare
WRITEOK — è coperto da `completeInterruptedUpdates`, che delivera le proposte
di fase 1 rimaste in sospeso sul vincitore.

**Total Order** — gli `UpdateID` sono assegnati da un solo coordinatore per
epoch (`lastAssignedId.nextInEpoch()`), gli epoch sono strettamente crescenti
(`newEpoch = max(visti)+1`), la history è append-only e il replay è ordinato
(`after()` preserva l'ordine, `completeInterruptedUpdates` ordina le chiavi).
Nessuna replica può quindi applicare `w'` prima di `w` se `id(w) < id(w')`.

---

## 14. Tabella riassuntiva dei timer

| Timer | Valore | Armato in | Cancellato da |
|-------|--------|-----------|---------------|
| heartbeat del coordinatore | `coordinatorBeatInterval` (1000 ms), periodico | `startHeartbeatLoop` `:262` | `cancelAllTimers`, `rearmHeartbeatTimeout` |
| `HeartbeatTimeout` | `3 × 1000 = 3000 ms` | `resetHeartbeatTimeout` `:281` | ogni `Heartbeat`/`UpdateMsg`/`WriteOk`; ingresso in ELECTION |
| `ForwardTimeout` | `3 × 1000 = 3000 ms` | `scheduleForwardTimeout` `:828` | arrivo dell'`UpdateMsg` con quel `reqId` |
| `UpdateTimeout` | `3 × 1000 = 3000 ms` | `scheduleUpdateTimeout` `:838` | `WriteOk` con quell'id; `Synchronization` |
| `ElectionAckTimeout` | `getMaxLatencyPlusTolerance()` = `20 + 10·N` ms | `rearmElectionAckTimeout` `:624` | `ElectionAck` |
| `GlobalElectionTimeout` | `N × getMaxLatencyPlusTolerance() × 2` | `rearmGlobalElectionTimeout` `:629` | `becomeWinner`, `onSynchronization` |

Con `N = 7`: ack-timeout ≈ 90 ms, global ≈ 1,26 s, detection ≈ 3 s.
Il budget dei test (`TestsCommons.getElectionMaxDelay`) è
`(3000 + 2·max_lat·N + 2·N·max_lat) × 5` ≈ 17 s per N=7: ampio margine.

---

## 15. Logging

Tutto passa da `Logger` (timestamp a precisione ms). **Non c'è un solo
`System.out.println` nel progetto**, nemmeno in `Main`: la traccia avverte che
le stampe possono interferire con i test automatici.

Formati prescritti dalla traccia §2 e dove sono prodotti:

| Formato richiesto | Dove |
|-------------------|------|
| `[Client X] requesting READ (<idx>) to <ReplicaID>` | `Client.java:51` |
| `[Client X] requesting WRITE (<idx>, <val>) to <ReplicaID>` | `Client.java:60` |
| `[Client X] READ complete (...)` / `WRITE complete (...)` | `AbstractClient` (codebase) |
| `[Client X] TIMEOUT READ/WRITE request to ...` | `AbstractClient` (codebase) |
| `[Replica id] applied update <epoch>:<seq> (<idx>, <val>)` | `Replica.java:459` |
| `[Replica id] CRASHED` | `AbstractReplica:372` (codebase) |

In più, log nostri sui passaggi chiave del protocollo: `UPDATE proposed`,
`ACK <id> to coordinator`, `quorum reached for <id> -> WRITEOK`,
`crashed while broadcasting ...`, `HEARTBEAT TIMEOUT`,
`ELECTION ACK TIMEOUT: suspecting k`, `ring lap complete, winner is k`,
`WON the election`, `SYNCHRONIZATION from k: epoch e, n update(s) to replay`,
`replaying n buffered write(s)`.

Il commento degli scenari di demo esce con il prefisso `[demo]`, così si
distingue a colpo d'occhio dai log degli attori.

---

## 16. Demo eseguibili

`Main.java` contiene i quattro scenari raccomandati dalla traccia §4
(*"three or four representative execution examples, including corner cases"*).
Ogni demo costruisce il proprio `ActorSystem`, lo guida dall'esterno con
richieste del client e comandi di crash, e lo termina prima che parta la
successiva: il log di uno scenario si legge da solo. `N = 5`, coordinatore
iniziale `0`.

```bash
./gradlew run                 # tutti e quattro in sequenza (~30 s)
./gradlew run --args="3"      # solo lo scenario 3
```

### Demo 1 — happy path
Tre write e una read da un client attaccato alla Replica 4. Si osservano gli
id consecutivi `<0,1> <0,2> <0,3>`, i cinque ACK, il `quorum reached`, e le
cinque repliche che applicano nello stesso ordine. La read finale legge `30`,
cioè l'ultima write dell'ordine totale.

### Demo 2 — crash di una replica non coordinatrice
Dopo una prima write, la Replica 3 riceve `Crash(Now, 0)`. La write successiva
raggiunge comunque il quorum (4 repliche vive su 5, ne bastano 3) e viene
applicata dalle sole repliche vive. Il secondo client, che continua a parlare
con la replica morta, va in `TIMEOUT READ` dopo 800 ms.

### Demo 3 — crash del coordinatore, elezione e sincronizzazione
Il coordinatore 0 muore fra due write. La write emessa subito dopo resta
bloccata sulla Replica 2, che l'aveva inoltrata. Dopo ~3 s scattano gli
`HEARTBEAT TIMEOUT`, parte l'elezione ad anello, tutte le repliche hanno lo
stesso `latestId` e il tie-break assegna la vittoria alla Replica 4; la
`SYNCHRONIZATION` apre l'epoch 1 e la Replica 2 rigioca la write bufferizzata,
che viene applicata come `<1,1>`. Il client riceve il suo `WRITE complete`
diversi secondi dopo averla emessa, senza aver ritentato nulla.

### Demo 4 — WRITEOK parzialmente disseminato (uniform agreement)
Il coordinatore è armato con `Crash(WriteOK, 2)`: raggiunge il quorum, invia il
WRITEOK a due repliche (sé stesso — che però è già morto quando gli tornerebbe
indietro — e la Replica 1) e muore a metà broadcast, lasciando il log
`crashed while broadcasting WriteOk(<0,1>)`. Da quel momento **una sola replica
al mondo ha applicato `<0,1>`**. L'elezione la premia proprio per questo
(`winner is 1`), e la sua `SYNCHRONIZATION` porta l'update alle altre tre, che
lo applicano. Il client, che era attaccato alla Replica 4, riceve il suo
`WRITE complete` e la read finale legge `99` da una replica che quel valore
l'ha conosciuto solo attraverso la sincronizzazione.

È lo scenario che dimostra dal vivo la property della traccia: *"if a replica a
applies an update w, then all correct replicas will eventually apply w"*.

---

## 17. Test: cosa esiste e cosa misura

**93 test, tutti verdi**, verificati su 3 esecuzioni consecutive complete
(`./gradlew test --rerun-tasks`), senza flakiness.

| Suite | Origine | Casi | Cosa verifica |
|-------|---------|------|---------------|
| `base/APICompliance` | codebase | 25 | conformità alle API e alle callback obbligatorie |
| `base/NoCrashes` | codebase | 4 | happy path con N ∈ {3, 7, 22} |
| `base/WithCrashes` | codebase | 4 | crash di più repliche e del coordinatore |
| `election/RingTopologyTest` | nostro | 22 | ring, successore, skip, wrap-around |
| `election/ElectionLogicTest` | nostro | 22 | vincitore, tie-break, payload, newEpoch |
| `election/SyncPlanTest` | nostro | 16 | diff, watermark, immutabilità |

**`APICompliance`** copre, oltre all'happy path: `replicasCrashNow`,
`crashReplicaAndTryRequests` (il client va in timeout con i campi giusti),
`callbackOnUpdateAppliedInvokedOnAllReplicas`,
`callbackOnUpdateAppliedOncePerWrite`,
`callbackOnElectionStartedInvokedCorrectly`,
`callbackOnElectionStartedCalledAtMostOncePerReplica`,
`callbackOnCoordinatorElectedAllAgree`,
`callbackOnCoordinatorElectedNewCoordAlsoCalls`.

**`WithCrashes`** copre due scenari parametrizzati:
- `nonCoordinatorsCrashClientWritesWaitsReads` (N ∈ {7, 22}): crashano
  `N/2 − 2` repliche non coordinatrici, la write deve comunque andare a buon
  fine e la read successiva leggere il valore scritto;
- `coordinatorCrashClientWritesWaitsReads` (N ∈ {7, 22}): crashano il
  coordinatore e altre due repliche **con `Crash.Type.Now`**, poi il client
  scrive: la write deve completare *dopo* l'elezione e il `WriteResult` deve
  riportare la replica contattata.

**I 60 unit test in `election/`** girano senza `ActorSystem`, quindi in
millisecondi e in modo deterministico. Coprono, fra l'altro: ordinamento e
deduplicazione del ring, giro completo che visita ogni replica esattamente una
volta, skip di due sospettati consecutivi e attraverso il wrap-around, id non
contigui; tie-break applicato *solo* fra le repliche più aggiornate,
indipendenza dall'ordine di iterazione della mappa, `newEpoch` quando un
partecipante ha visto un epoch più alto del vincitore; diff che attraversa il
confine di epoch (update orfano), replica più avanti del vincitore,
immutabilità dei risultati.

Quello che **non** c'è è una suite end-to-end sui crash a punti specifici del
protocollo: vedi §18.2.

---

## 18. Cosa manca da fare

In ordine di priorità.

### 18.1 Report LaTeX (Sprint 5.1) — **bloccante per la consegna**

`report/main.tex` compila ma le tre sezioni sono **file di una riga con il solo
`\section{}`**: `01_structure.tex`, `02_design.tex`, `03_implementation.tex`.
Serve scrivere 3-4 pagine (max 6, oltre le quali il progetto viene rifiutato
d'ufficio) in inglese, rispondendo alle domande delle slide:

- scelte architetturali (perché la logica di elezione è pura e separata dalla FSM);
- gestione dei timeout e valori scelti (§14) con la giustificazione del "no
  false positives";
- topologia del ring e perché la sequenza di hop invece dell'aritmetica modulare;
- trattamento degli update orfani e ordine delle fasi in `becomeWinner`;
- motivazione del tie-break e della sentinella `NONE`;
- perché `Synchronization` è un broadcast unico calcolato sul watermark;
- come è istrumentato il crash a metà broadcast (§10) e cosa dimostra la Demo 4;
- assunzioni aggiuntive (§19).

Va incluso anche il disclaimer sull'uso di assistenza AI.

### 18.2 Test dei corner case dello Sprint 4

I test della codebase passano tutti e l'istrumentazione per innescare ogni
corner case **c'è** (§10), ma i sei scenari elencati in `ROADMAP.md` → Sprint 4
non hanno un test automatico dedicato:

| Corner case | Come innescarlo | Stato |
|-------------|-----------------|-------|
| 1. coordinatore crasha durante il broadcast di UPDATE | `Crash(Update, k)`, `k < N` | innescabile, nessun test |
| 2. coordinatore crasha dopo WRITEOK parziale | `Crash(WriteOK, k)` | ✅ dimostrato dalla Demo 4, nessun test |
| 3. due nodi consecutivi crashano durante l'elezione | `Crash(Now, 0)` su id adiacenti | solo unit test su `RingTopology` |
| 4. vincitore crasha prima della `Synchronization` | `Crash(Election, k)` | logica presente, nessun test |
| 5. replica crasha dopo l'ACK, prima del WRITEOK | `Crash(Update, 0)` | innescabile, nessun test |
| 6. client contatta una replica crashata | `Crash(Now, 0)` | ✅ `APICompliance.crashReplicaAndTryRequests` |

Conviene aggiungere una suite `src/test/java/it/unitn/ds/scenarios/` con questi
casi: è anche il materiale migliore per rispondere alle domande dell'orale.

### 18.3 Pulizia prima della consegna

- Codice mai usato: `UpdateID.nextEpoch()`, `UpdateHistory.asList()`/`size()`/
  `isEmpty()`, `SyncPlan.missingFor` (usato solo indirettamente),
  `Election.initiatorId` (trasportato ma non usato per decidere).
  Non è un problema, ma va saputo se qualcuno lo chiede all'orale.
- Alcuni typo nei commenti (`aliv`, `lready`, `Puleld`, `awating`,
  `callbackOnElectionSTarted`).
- `CONTRACT_PHASE0.md` §9 ha ancora la checklist delle decisioni D1-D5
  formalmente aperta, benché tutte e cinque siano di fatto chiuse nel codice.

### 18.4 Consegna (Sprint 5.3)

- [ ] `./gradlew test` verde su clone pulito (verificato oggi sul working tree);
- [ ] report `.pdf` autocontenuto;
- [ ] archivio `tar -czvf CognomeACognomeB.tgz CognomeACognomeB/` con sorgenti
      + report, **senza** i PDF del prof in `docs/`;
- [ ] prenotazione dello slot via mail a Picco + Pasquali + Genetti;
- [ ] indicare in-person vs online;
- [ ] preparare i 12 minuti (timer rigido) + Q&A.

---

## 19. Limitazioni note e assunzioni da dichiarare nel report

Sono scelte difendibili, ma vanno **dichiarate** (traccia §2: *"It is important
to state all additional assumptions in the report"*).

1. **I messaggi già accodati sopravvivono al crash.** I `NetworkChannel` sono
   figli della replica e non vengono fermati da `triggerCrash()`, quindi ciò
   che il mittente aveva già consegnato al canale arriva comunque. Il crash
   impedisce di *iniziare* nuovi invii, non di completare quelli in volo — ed è
   proprio questo che rende il broadcast parziale di §10 un troncamento del
   ciclo di invio e non una cancellazione di messaggi già partiti.
2. **`ElectionAck` è vuoto (D1)**: `onElectionAck` (`:591`) cancella il timer
   corrente senza verificare da chi arriva. Un ack in ritardo di un successore
   già saltato può quindi cancellare un timer appena riarmato per un altro
   successore; l'effetto peggiore è un round più lento, coperto dal
   `GlobalElectionTimeout`. Correlare l'ack con l'id del mittente sarebbe una
   modifica di due righe se emergesse il problema.
3. **Una `Synchronization` molto in ritardo può interrompere un'elezione.** La
   guardia sui duplicati non scatta quando la replica sta partecipando a un
   round: se l'annuncio di un coordinatore ormai morto arriva in quel momento,
   la replica esce dall'elezione e lo adotta, per poi riaccorgersi della sua
   morte al successivo `HeartbeatTimeout`. Il sistema converge lo stesso, con
   un'elezione in più. Non l'abbiamo blindato perché richiederebbe di rifiutare
   annunci da repliche sospettate, e la finestra è larga quanto un hop.
4. **Una replica viva ma lenta può essere sospettata** se non ack-a entro
   `getMaxLatencyPlusTolerance()`. Verrebbe esclusa dal payload
   dell'`Election` e quindi dal calcolo del watermark di `missingForAll`,
   restando potenzialmente indietro. La traccia però assume esplicitamente che
   la crash detection sia accurata, quindi lo scenario è fuori dal modello.
5. **Le read durante l'elezione sono servite localmente** e possono restituire
   un valore non ancora aggiornato: consistenza sequenziale, non
   linearizzabilità — che è quanto la traccia richiede.
6. **Il vincitore delivera anche update mai arrivati al quorum.**
   `completeInterruptedUpdates` applica tutte le proposte di fase 1 rimaste
   pendenti, comprese quelle che non avevano raggiunto `⌊N/2⌋+1` ack. Non viola
   nessuna delle quattro proprietà (nessuno le ha "non-consegnate" in modo
   osservabile) ed è la scelta conservativa: preferisce completare piuttosto
   che scartare.
7. **La membership è statica**: `suspected` cresce soltanto, coerentemente con
   *"Replicas fail by crashing and do not recover"*.

---

## 20. Come compilare ed eseguire

Serve solo un JDK 17+; il wrapper gestisce Gradle.

```bash
./gradlew build                                  # compila tutto ed esegue i test
./gradlew test                                   # solo i test (~2 minuti)
./gradlew test --tests "*NoCrashes*"             # happy path
./gradlew test --tests "*WithCrashes*"           # crash + elezione
./gradlew test --tests "*APICompliance*"         # conformità alle API
./gradlew test --tests "it.unitn.ds.election.*"  # unit test puri (millisecondi)
./gradlew test --rerun-tasks                     # forza la riesecuzione (utile per la flakiness)
./gradlew clean

./gradlew run                                    # i quattro scenari di demo (~30 s)
./gradlew run --args="4"                         # solo lo scenario 4
```

Report HTML dei test: `build/reports/tests/test/index.html`.
Durante i test i log sono disabilitati (`TestsCommons.DO_PRINTS = false`)
perché, come avverte la traccia, le stampe possono interferire con l'esito;
nelle demo invece sono attivi su stdout (`Logger.setDestinationStdout()`).
