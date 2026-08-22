# Stato implementativo — guida completa al codice

Documento di stato **e** manuale di lettura del progetto **Quorum-Based Total
Order Broadcast** (Distributed Systems 2025-2026). Descrive **cosa è
implementato, in quale file e riga vive, come funziona e perché è corretto**,
confrontandolo punto per punto con la traccia `docs/ds1_project_2026.pdf`.

La Parte II è una **guida file-per-file e metodo-per-metodo** di tutti i 38
sorgenti Java del repository: è pensata per essere letta prima dell'orale, dove
può essere chiesto di aprire un file qualunque e spiegarlo.

Identificatori, log e report restano in inglese; la prosa è in italiano come da
convenzione interna.

- Roadmap per sprint con criteri di uscita → [`ROADMAP.md`](ROADMAP.md)
- Contratto di interfaccia fra i due flussi di lavoro → [`ROADMAP.md`](ROADMAP.md) → "Fase 0"
  (il file `CONTRACT_PHASE0.md` è stato assorbito lì e rimosso, essendo ormai
  interamente chiuso dal codice)

**Ultimo aggiornamento**: 2026-08-22, su `main` (base `d854f59` più le quattro
correzioni dell'audit descritte in [§21](#21-osservazioni-emerse-dallaudit-del-2026-08-22)).
`sprint3-election` è già mergiato in `main` (`6c01abe`) e non contiene lavoro
ulteriore: `main` è l'unica linea di sviluppo viva.

**Verifica eseguita per questo aggiornamento**: `./gradlew build` e
`./gradlew test --rerun-tasks` completi, **98 test su 98 verdi**, zero
fallimenti, zero skip, zero warning di compilazione — sia prima sia dopo le
correzioni dell'audit. I quattro scenari di `./gradlew run` girano end-to-end.

> I numeri di riga citati fra parentesi (es. `Replica.java:456`) si riferiscono
> allo stato del codice dopo le correzioni dell'audit. Se il codice viene
> toccato vanno riverificati.

---

## Indice

- **Parte I — Quadro generale**
  - §1 [Sintesi: dove siamo](#1-sintesi-dove-siamo)
  - §2 [Tracciabilità requisiti → codice](#2-tracciabilità-requisiti--codice)
  - §3 [Layout del repository e inventario dei file](#3-layout-del-repository-e-inventario-dei-file)
- **Parte II — Guida al codice, file per file**
  - §4 [Codebase obbligatoria (intoccabile)](#4-codebase-obbligatoria-intoccabile)
  - §5 [Modello dati](#5-modello-dati)
  - §6 [Catalogo dei messaggi (17 file)](#6-catalogo-dei-messaggi-17-file)
  - §7 [Package `election/` — logica pura](#7-package-election--logica-pura)
  - §8 [`Client.java`](#8-clientjava)
  - §9 [`Replica.java` — anatomia completa](#9-replicajava--anatomia-completa)
  - §10 [`Main.java`](#10-mainjava)
  - §11 [I file di test](#11-i-file-di-test)
- **Parte III — Il protocollo in azione**
  - §12 [Percorso di una READ](#12-percorso-di-una-read)
  - §13 [Percorso di una WRITE (two-phase)](#13-percorso-di-una-write-two-phase)
  - §14 [Ciclo di vita di un'elezione](#14-ciclo-di-vita-di-unelezione)
  - §15 [Come sono garantite le proprietà di safety](#15-come-sono-garantite-le-proprietà-di-safety)
  - §16 [Tabella riassuntiva dei timer](#16-tabella-riassuntiva-dei-timer)
  - §17 [Logging](#17-logging)
- **Parte IV — Verifica**
  - §18 [Test: cosa esiste e cosa misura](#18-test-cosa-esiste-e-cosa-misura)
  - §19 [Demo eseguibili](#19-demo-eseguibili)
- **Parte V — Stato e residui**
  - §20 [Cosa manca da fare](#20-cosa-manca-da-fare)
  - §21 [Osservazioni emerse dall'audit del 2026-08-22](#21-osservazioni-emerse-dallaudit-del-2026-08-22)
  - §22 [Limitazioni note e assunzioni da dichiarare nel report](#22-limitazioni-note-e-assunzioni-da-dichiarare-nel-report)
  - §23 [Come compilare ed eseguire](#23-come-compilare-ed-eseguire)

---

# Parte I — Quadro generale

## 1. Sintesi: dove siamo

**Il protocollo è completo, verificato e dimostrabile.** Tutti i requisiti
funzionali della traccia sono implementati; tutti i 98 test del repository
passano; i corner case del fault model hanno ora una suite dedicata.

| Sprint | Contenuto | Stato |
|--------|-----------|-------|
| 0 | Setup repo, Gradle wrapper, dipendenze | ✅ completato |
| 1 | Modello dati, messaggi, happy path client/replica | ✅ completato |
| 2 | Heartbeat, crashed mode, crash istrumentati, timeout | ✅ completato |
| 3 [B] | Logica pura di ring/vincitore/diff (`election/`) | ✅ completato |
| 3 [A+B] | Integrazione FSM: trigger, ack-skip, Synchronization, callback | ✅ completato |
| 4 | Corner case del fault model | ✅ **completato** (`scenarios/CornerCases`, 5 casi) |
| 5.1 | Report LaTeX | ❌ **da fare — unico blocco alla consegna** |
| 5.2 | Demo eseguibili | ✅ completato |
| 5.3 | Consegna (archivio, slot, presentazione) | ⚠️ da eseguire |
| 5.4 | Rifiniture dell'audit (wrapper, refusi, crash counter, elezione stantia) | ✅ completato |

In sostanza: **il codice è finito; manca il report e l'atto materiale della
consegna**. Il dettaglio operativo è in [§20](#20-cosa-manca-da-fare); le
imperfezioni non bloccanti trovate durante l'audit sono in
[§21](#21-osservazioni-emerse-dallaudit-del-2026-08-22).

---

## 2. Tracciabilità requisiti → codice

Ogni riga è un requisito della traccia; la colonna "dove" indica il punto
esatto dell'implementazione. Nessuna riga è rossa fra i requisiti di codice.

| Requisito (traccia)                                                    | Dove                                                     | Stato |
|------------------------------------------------------------------------|----------------------------------------------------------|-------|
| §1 Ogni replica tiene una copia di `P[]`                                | `Replica.java:54` (`positions[POSITIONS_LIST_LENGTH]`)   | ✅ |
| §1 Read servita localmente dalla replica contattata                     | `Replica.java:331` `onClientRead`                        | ✅ |
| §1 Write inoltrata al coordinatore dalla replica contattata             | `Replica.java:360` `submitClientWrite`                   | ✅ |
| §1 UPDATE broadcast dal coordinatore                                    | `Replica.java:393` `startUpdate`                         | ✅ |
| §1 ACK da ogni replica                                                  | `Replica.java:402` `onUpdateMsg`                         | ✅ |
| §1 Quorum `⌊N/2⌋+1`, coordinatore incluso                               | `Replica.java:420` `onUpdateAck` + `:802` `quorum()`     | ✅ |
| §1 WRITEOK broadcast e applicazione locale                              | `Replica.java:434` `onWriteOk` → `:456` `applyUpdate`    | ✅ |
| §1 Id univoco `⟨e,i⟩`, epoch monotona, sequence azzerata a ogni epoch   | `UpdateID.java`, `Replica.java:394`, `:662`              | ✅ |
| §1 History degli update mantenuta per il recovery                       | `UpdateHistory.java`, `Replica.java:61`                  | ✅ |
| §1 Timeout su WRITEOK dopo aver ricevuto UPDATE                         | `Replica.java:876` `scheduleUpdateTimeout` + `:318`      | ✅ |
| §1 Timeout su write inoltrata (coordinatore non avvia il broadcast)     | `Replica.java:866` `scheduleForwardTimeout` + `:309`     | ✅ |
| §1 HEARTBEAT periodico dal coordinatore                                 | `Replica.java:262` `startHeartbeatLoop`                  | ✅ |
| §1 Ring definito dall'ordine degli id                                   | `election/RingTopology.java:31` `order`                  | ✅ |
| §1 ELECTION accumula l'update più recente noto a ciascuna replica       | `messages/Election.java` + `ElectionLogic.withEntry`     | ✅ |
| §1 "se non sta già partecipando, aggiunge la propria informazione"      | `ElectionLogic.java:89` (`putIfAbsent`)                  | ✅ |
| §1 ACK hop-by-hop dell'ELECTION                                         | `Replica.java:534` (ack immediato) + `:656`              | ✅ |
| §1 Timeout sull'ACK → skip del successore                               | `Replica.java:628` `onElectionAckTimeout`                | ✅ |
| §1 Vincitore = update più recente, tie-break su id                      | `ElectionLogic.java:35` `winner`                         | ✅ |
| §1 SYNCHRONIZATION per annunciare la leadership                         | `Replica.java:702`                                       | ✅ |
| §1 Il nuovo coordinatore allinea gli altri prima di riprendere le write | `Replica.java:679` `becomeWinner` (ordine delle fasi)    | ✅ |
| §1 Completamento degli update interrotti (partial dissemination)        | `Replica.java:722` `completeInterruptedUpdates`          | ✅ |
| §2 Akka, attori, Java                                                   | tutto il progetto (Akka classic 2.6.13, Java 21)         | ✅ |
| §2 Uso obbligatorio della codebase, `createBaseReceiveBuilder()`        | `Replica.java:210`, `:246`; `Client.java:67`             | ✅ |
| §2 Canali FIFO con latenza random                                       | `NetworkChannel.java`, usato via `tell()`                | ✅ |
| §2 Nessuno stato mutabile condiviso; messaggi immutabili                | `messages/*` tutti `final`+`Serializable`+copie difensive| ✅ |
| §2 Maggioranza stretta sempre viva                                      | assunzione rispettata da tutti i test e le demo          | ✅ |
| §2 Crashed mode senza `stop()`                                          | `Replica.java:181` `crashed()`                           | ✅ |
| §2 Crash istrumentati per tipo di messaggio                             | `Replica.java:932` `checkCrashCondition`                 | ✅ |
| §2 Crash **a metà** del broadcast di UPDATE / WRITEOK                   | `Replica.java:823` `broadcast(msg, crashPoint)`          | ✅ |
| §2 Log con timestamp e formato prescritto                               | `Logger.java` + `log()` in tutta `Replica`               | ✅ |
| §2 Sequenze di write interlacciate a crash con read concorrenti         | `Main.java` (demo 2-4), `WithCrashes`, `NoCrashes`       | ✅ |
| §3 Report 3-4 pagine in inglese                                         | `report/`                                                | ❌ **vuoto** |
| §4 3-4 scenari di esecuzione rappresentativi                            | `Main.java` (4 demo) + `scenarios/CornerCases` (5 test)  | ✅ |

---

## 3. Layout del repository e inventario dei file

```
SD_PROJECT/
├── build.gradle                # Akka classic 2.6.13 + JUnit 6, plugin application, main = it.unitn.ds.Main
├── gradlew, gradlew.bat,       # wrapper Gradle 9.2.1 — committati, così un clone
│   gradle/wrapper/             #   pulito compila senza Gradle installato (§21.1)
├── README.md                   # istruzioni rapide
├── ROADMAP.md                  # piano per sprint con criteri di uscita
├── IMPLEMENTATION_STATUS.md    # questo documento
├── docs/                       # traccia, slide, guida di pianificazione interna (NON consegnare)
├── report/                     # skeleton LaTeX: main.tex compila, le 3 sezioni sono vuote
└── src/
    ├── main/java/it/unitn/ds/          → 25 file
    │   ├── AbstractClient.java         # CODEBASE — non modificare    (241 righe)
    │   ├── AbstractReplica.java        # CODEBASE — non modificare    (430)
    │   ├── Logger.java                 # CODEBASE — logging timestamped (132)
    │   ├── NetworkChannel.java         # CODEBASE — canale FIFO ritardato (88)
    │   ├── Main.java                   # NOSTRO — i quattro scenari di demo (255)
    │   ├── Client.java                 # NOSTRO — client con timeout per richiesta (140)
    │   ├── Replica.java                # NOSTRO — il cuore del progetto (881)
    │   ├── UpdateID.java               # NOSTRO — ⟨epoch, sequence⟩ (57)
    │   ├── Update.java                 # NOSTRO — entry della history (39)
    │   ├── UpdateHistory.java          # NOSTRO — log append-only (67)
    │   ├── election/                   # NOSTRO — logica pura, senza attori
    │   │   ├── RingTopology.java       (79)
    │   │   ├── ElectionLogic.java      (113)
    │   │   └── SyncPlan.java           (72)
    │   └── messages/                   # NOSTRO — 17 POJO immutabili (18-38 righe l'uno)
    └── test/java/it/unitn/ds/          → 8 file, 98 casi
        ├── TestsCommons.java           # CODEBASE — helper e costanti di timing (125)
        ├── base/                       # CODEBASE — test obbligatori
        │   ├── APICompliance.java      (25 casi)
        │   ├── NoCrashes.java          (4 casi)
        │   └── WithCrashes.java        (4 casi)
        ├── election/                   # NOSTRO — unit test puri, senza ActorSystem
        │   ├── RingTopologyTest.java   (22 casi)
        │   ├── ElectionLogicTest.java  (22 casi)
        │   └── SyncPlanTest.java       (16 casi)
        └── scenarios/
            └── CornerCases.java        # NOSTRO — 5 casi end-to-end sul fault model
```

**Totale sorgenti Java**: 4 861 righe, di cui ~1 200 di codebase intoccabile.

Convenzione di ownership (dalla Fase 0, ora in `ROADMAP.md` §F0.1):
`AbstractReplica`, `AbstractClient`, `Logger`, `NetworkChannel` e tutto
`src/test/.../base/` + `TestsCommons` arrivano dalla codebase obbligatoria
Genetti/Pasquali e **non si modificano**. Tutto il resto è nostro.

---

# Parte II — Guida al codice, file per file

## 4. Codebase obbligatoria (intoccabile)

### 4.1 `AbstractReplica.java` (430 righe)

Classe base astratta di ogni replica; estende `akka.actor.AbstractActor`.
Definisce **le regole del gioco** cui `Replica` deve sottostare.

**Costanti** (`:15-18`) — `MIN_LATENCY = 5`, `MAX_LATENCY = 20`,
`COORDINATOR_BEAT_INTERVAL = 1000`, `POSITIONS_LIST_LENGTH = 100`.

**Campi** (`:21-32`) — `final int id` (identità della replica, visibile alle
sottoclassi), `boolean initialized`, i parametri di latenza, la mappa
`channels` dei `NetworkChannel` creati pigramente, e `Optional<ActorRef>
listener` (la probe `TestKit` nei test, `Optional.empty()` in produzione).

**Metodi chiave**

| Metodo | Riga | Cosa fa e perché ci interessa |
|--------|------|------------------------------|
| `getCoordinatorBeatInterval()` | 53 | periodo dell'heartbeat; base di tutti i timeout di detection |
| `setNetworkLatency` / `getMinLatency` / `getMaxLatency` | 62-81 | parametri del canale |
| `getMaxLatencyPlusTolerance()` | 88 | `maxLatency + maxLatency/2 × N` — stima "un hop più tolleranza proporzionale alla taglia"; è il valore usato per l'`ElectionAckTimeout` |
| **`tell(Serializable, ActorRef)`** | 96 | **l'unico modo lecito di inviare**. Crea pigramente un `NetworkChannel` per destinazione (figlio del mittente, nominato `channel_to_<dst>`) e ci accoda il messaggio. Da qui derivano FIFO per coppia (mittente, destinatario) e ritardo casuale in `[5,20) ms` per hop |
| `log` / `debug` | 108-114 | prefissano `[Replica <id>] ` e passano da `Logger` |
| `createBaseReceiveBuilder()` | 385 | aggancia `Crash` (sempre) e `InitSystem` (solo finché `!initialized`). **Ogni `Receive` della replica deve partire da qui**, altrimenti i comandi di crash non arrivano più |

**Messaggi API annidati**

- `InitSystem(Map<Integer,ActorRef> group, int coordinator_id)` (`:132`) — la
  mappa è resa `unmodifiable` su una copia nel costruttore.
- `Crash(Crash.Type type, int after_n_messages_of_type)` (`:168`) con
  `Type ∈ {Now, Heartbeat, Update, WriteOK, Election}` (`:173`). Semantica:
  processa `n` messaggi di quel tipo, crasha all'`(n+1)`-esimo.
- `CoordinatorElected`, `UpdateApplied`, `ElectionStarted` (`:250`, `:274`,
  `:300`) — eventi che le callback notificano al listener; i test li
  confrontano con `equals`.

**Callback obbligatorie**, tutte `final` (non sovrascrivibili) — loggano e
notificano il listener:

- `callbackOnCoordinatorElected(int)` (`:337`)
- `callbackOnUpdateApplied(int index, int value)` (`:349`)
- `callbackOnElectionStarted(int crashedCoordinatorId)` (`:361`)

**Wrapper handler** — `onCrashMsg` (`:370`) chiama `crash(...)` **e poi** logga
`CRASHED` e notifica il listener. Conseguenza da conoscere: il log `CRASHED:
WriteOK (2)` compare alla *ricezione del comando*, non quando la replica muore
davvero. Nei log della Demo 4 si vede infatti `CRASHED` all'inizio e la morte
effettiva qualche secondo dopo, a metà del broadcast del WRITEOK.

**Metodi astratti** che `Replica` deve implementare: `getSystemNumberOfActors()`,
`crash(Crash)`, `initSystem(InitSystem)`.

### 4.2 `AbstractClient.java` (241 righe)

Base astratta del client. Espone:

- i messaggi che il **test harness** manda al client:
  `ReadRequest(index[, replica])`, `WriteRequest(index, value[, replica])`;
- i tipi risultato `ReadResult` / `WriteResult` (sottoclassi di `Result`, con
  `success`, `index`, `value`, `fromReplica`) e `ReadTimeout` / `WriteTimeout`;
- le quattro callback obbligatorie `final`: `callbackOnReadResult`,
  `callbackOnWriteResult`, `callbackOnReadTimeout`, `callbackOnWriteTimeout`
  (`:163-192`), che loggano nel formato prescritto dalla traccia §2;
- `createBaseReceiveBuilder()` (`:235`) che aggancia `ReadRequest`/`WriteRequest`
  a `onReadRequest`/`onWriteRequest` (`:198`, `:208`), i quali risolvono la
  replica bersaglio (esplicita nel messaggio, altrimenti la
  `defaultTargetReplica`) e chiamano i due metodi astratti `sendRead` /
  `sendWrite`.

**Nota architetturale importante**: `AbstractClient` **non** ha un helper
`tell(...)` né una mappa di canali. Il dialogo client → replica avviene quindi
con `ActorRef.tell` diretto (`Client.java:52`, `:61`), mentre il ritorno
replica → client passa dal `NetworkChannel`. È il design della codebase, ma va
dichiarato nel report come assunzione (vedi §22.8).

### 4.3 `NetworkChannel.java` (88 righe)

Attore che emula un collegamento di rete verso **una** destinazione fissa.

- `props(destination, minLatency, maxLatency)` (`:42`).
- `createReceive()` (`:48`): `Deliver` (self-message) → `onDeliver`; qualunque
  altro messaggio → `onEnqueue`.
- `onEnqueue` (`:55`) accoda la coppia `(messaggio, mittente originale)` e, se
  non c'è già una consegna in volo, ne schedula una.
- `onDeliver` (`:62`) estrae la testa della coda, la inoltra alla destinazione
  **preservando il mittente originale** (`destination.tell(msg, originalSender)`
  — è così che `getSender()` nella replica ritorna il vero mittente e non il
  canale), e ripianifica se la coda non è vuota.
- `scheduleNextDelivery` (`:79`) usa un ritardo casuale in `[min, max)` ms.

**Proprietà che il protocollo usa**: consegna **FIFO** per coppia (mittente,
destinatario) — una singola coda servita in sequenza — e ritardo casuale ma
limitato, come richiesto dalla traccia §2.

**Conseguenza sul fault model**: i canali sono **figli del mittente e non
vengono fermati** da `triggerCrash()`. Ciò che era già stato consegnato al
canale prima del crash arriva comunque a destinazione. Il crash impedisce di
*iniziare* nuovi invii, non di completare quelli in volo (vedi §22.1).

### 4.4 `Logger.java` (132 righe)

Logger statico, sincronizzato su un lock, con timestamp
`yyyy-MM-dd HH:mm:ss.SSS` (precisione al millisecondo, come richiesto dalla
traccia §2).

- Destinazione commutabile: `setDestinationStdout()` / `setDestinationFile(name)`.
- `setDebugEnabled(boolean)`, `setLoggingEnabled(boolean)`, `disable()`,
  `enable()`.
- `log(String)` scrive a livello `INFO`; `debug(String)` solo se il debug è
  abilitato.
- `write(...)` (`:107`) formatta `"<timestamp> [<LEVEL>] <messaggio>"`, scrive e
  fa **flush immediato**.

Nei test `TestsCommons.createTestSystem` chiama `Logger.disable()` quando
`DO_PRINTS == false`, perché — avverte la traccia — le stampe possono alterare
le finestre temporali dei test.

---

## 5. Modello dati

### 5.1 `UpdateID.java` — `⟨epoch, sequence⟩` (57 righe)

`final`, `Serializable`, `Comparable<UpdateID>`, campi `public final int epoch`
e `int sequence`.

| Membro | Riga | Note |
|--------|------|------|
| `nextInEpoch()` | 25 | `⟨e, i+1⟩` — usato dal coordinatore a ogni proposta (`Replica.java:394`) |
| `nextEpoch()` | 30 | `⟨e+1, 0⟩` — **non usato** dal codice attuale: il nuovo epoch si calcola su tutto il payload dell'elezione, non sul solo id del vincitore (vedi `ElectionLogic.newEpoch`) |
| `compareTo` | 35 | lessicografico: prima `epoch`, poi `sequence`. Un epoch più alto batte qualunque sequence |
| `equals` / `hashCode` | 41/49 | necessari: `UpdateID` è chiave di `HashMap` in `ackCounts`, `pendingUpdates`, `updateTimeouts`, `committed` |
| `toString` | 54 | `<e,i>` — è il formato che si legge nei log |

Traccia §1: *"Each update is uniquely identified by a pair ⟨e,i⟩ … Epoch
numbers increase monotonically … The sequence number is reset to 0 at the
beginning of each epoch"*.

### 5.2 `Update.java` — `(UpdateID id, int index, int value)` (39 righe)

Entry **deliverata** della history. Immutabile, `Serializable`, con
`equals`/`hashCode`/`toString`.

Scelta deliberata: **nessun `ActorRef` dentro**. È la rappresentazione
persistente e serializzabile del log, indipendente da chi l'ha applicata; i
dati di routing (client, replica contattata, `reqId`) viaggiano in `UpdateMsg`,
non qui. Per questo la stessa istanza può essere rispedita tale e quale dentro
una `Synchronization` senza portarsi dietro riferimenti a attori.

### 5.3 `UpdateHistory.java` — log append-only (67 righe)

Wrapper su un `ArrayList<Update>` che espone **solo append**, mai rimozioni né
riordini.

| Metodo | Riga | Usato da |
|--------|------|----------|
| `append(Update)` | 30 | `Replica.applyUpdate` (`:458`) — unico chiamante |
| `size()` / `isEmpty()` | 34/38 | non usati dal protocollo (API di comodo) |
| `latest()` | 42 | solo internamente da `latestId()` |
| `latestId()` | 46 | `ElectionLogic.latestOf` e `Replica.alreadyDelivered` |
| `after(UpdateID threshold)` | 55 | base del diff di sincronizzazione (`SyncPlan.missingFor`); ritorna una lista `unmodifiable` delle entry **strettamente** più recenti, in ordine |
| `asList()` | 50 | snapshot `unmodifiable`; non usato dal protocollo |

L'istanza è mutabile per il bookkeeping in-process ma vive **dentro un solo
attore**: non viene mai spedita, quindi non viola il divieto di stato mutabile
condiviso. Ciò che viaggia sono le `List<Update>` immutabili prodotte da
`after()`.

---

## 6. Catalogo dei messaggi (17 file)

Tutte le classi in `messages/` sono `final`, `Serializable`, con soli campi
`public final`, nessun setter, `toString()` leggibile, e collezioni **copiate
difensivamente** e wrappate `unmodifiable*` nel costruttore
(traccia §2: *"any shared objects must be immutable"*).

> **Nota di nomenclatura**: il broadcast di fase 1 si chiama `UpdateMsg` e non
> `Update` per non collidere con la data class della history. `UpdateMsg`
> *wrappa* un `Update` immutabile, così la stessa istanza può essere appesa
> alla history senza riallocazioni.

### 6.1 Client ↔ replica (4 file)

| Classe | Direzione | Campi | Prodotto da → consumato da |
|--------|-----------|-------|----------------------------|
| `ClientRead` | client → replica | `long reqId`, `int index` | `Client.sendRead:52` → `Replica.onClientRead:331` |
| `ClientWrite` | client → replica | `long reqId`, `int index`, `int value` | `Client.sendWrite:61` → `Replica.onClientWrite:347` / `onClientWriteDuringElection:354` |
| `ReadReply` | replica → client | `reqId`, `index`, `value`, `int fromReplica` | `Replica.onClientRead:335` → `Client.onReadReply:75` |
| `WriteReply` | replica → client | `reqId`, `index`, `value`, `int fromReplica` | `Replica.applyUpdate:468` → `Client.onWriteReply:81` |

Il `reqId` è un contatore locale al client (`Client.java:28`) e serve ad
**accoppiare risposta e timeout alla richiesta che li ha generati** quando ci
sono più richieste in volo. Per questo viaggia anche dentro `ForwardWrite` e
`UpdateMsg`: quando il `WriteOk` torna indietro, la replica contattata sa a chi
rispondere e con quale id.

`fromReplica` è **sempre** l'id della replica che il client ha contattato, mai
quella del coordinatore: è la regola 11 della codebase, verificata da
`APICompliance.oneClientWriteWaitRead` e da `WithCrashes`.

### 6.2 Protocollo two-phase (4 file)

| Classe | Direzione | Campi | Note |
|--------|-----------|-------|------|
| `ForwardWrite` | replica → coordinatore | `index`, `value`, `ActorRef client`, `int contactedReplicaId`, `long reqId` | `contactedReplicaId` è indispensabile: il `WriteResult` finale deve avere `fromReplica` = replica *contattata*, non il coordinatore |
| `UpdateMsg` | coordinatore → repliche | `Update update`, `ActorRef client`, `int contactedReplicaId`, `long reqId` | fase 1; i `requireNonNull` nel costruttore fanno fallire subito un uso scorretto |
| `UpdateAck` | replica → coordinatore | `UpdateID id` | fase 1, risposta |
| `WriteOk` | coordinatore → repliche | `UpdateID id` | fase 2; porta solo l'id perché il payload è già in `pendingUpdates` di ogni replica |

### 6.3 Liveness ed elezione (4 file)

| Classe | Direzione | Campi |
|--------|-----------|-------|
| `Heartbeat` | coordinatore → repliche | — (marker) |
| `Election` | replica → replica (ring) | `int initiatorId`, `Map<Integer,UpdateID> latestPerReplica` — copia difensiva + `unmodifiableMap` nel costruttore |
| `ElectionAck` | replica → replica | — (decisione D1: ack vuoto, hop-by-hop) |
| `Synchronization` | nuovo coordinatore → repliche | `int newCoordinatorId`, `int newEpoch`, `List<Update> pendingUpdates` — copia + `unmodifiableList` |

`Election.initiatorId` viene trasportato e propagato (`Replica.java:572`) e
compare nel `toString`, ma **non partecipa a nessuna decisione**: il vincitore
è funzione del solo `latestPerReplica`. È informazione diagnostica, utile nei
log e per un'eventuale estensione.

### 6.4 Timer self-schedulati (5 file)

Sono messaggi che una replica manda **a sé stessa** tramite lo scheduler di
Akka (`Replica.scheduleSelf:845`), mai attraverso un canale di rete.

| Classe | Campi | Chi lo arma e quando |
|--------|-------|----------------------|
| `HeartbeatTimeout` | — | ogni non-coordinatore; riarmato a ogni `Heartbeat`/`UpdateMsg`/`WriteOk` ricevuto |
| `ForwardTimeout` | `long reqId`, `int index`, `int value` | replica che ha inoltrato una write e non vede partire il broadcast |
| `UpdateTimeout` | `UpdateID id` | replica che ha ack-ato la fase 1 e non vede il `WriteOk` |
| `ElectionAckTimeout` | `int successorId` | mittente di una `Election` in attesa dell'ack del successore |
| `GlobalElectionTimeout` | — | rete anti-livelock, armata all'ingresso in ELECTION |

---

## 7. Package `election/` — logica pura

Tre classi `final`, costruttore privato, **soli metodi `static`, nessuno
stato**. Sono testabili senza `ActorSystem` (i 60 unit test girano in ~130 ms
totali e sono deterministici) e fungono da "seam" esplicito fra la FSM degli
attori e i dati del protocollo. Tutti i metodi validano gli argomenti con
`Objects.requireNonNull` / `IllegalArgumentException`, così un errore d'uso
esplode subito invece di propagarsi come dato sbagliato.

### 7.1 `RingTopology.java` (79 righe)

Traccia §1: *"The logical ring is defined by ordering replicas according to
their identifiers"*.

**`order(Collection<Integer> memberIds)` → `List<Integer>`** (`:31`)
Ordine canonico del ring: lista crescente `unmodifiable`, costruita passando
per un `TreeSet` — **ordina e deduplica in un colpo solo**, così
`group.keySet()` (una `HashSet`) e una lista arbitraria producono esattamente
lo stesso ring. Usata anche da `Replica.broadcast` per rendere riproducibile
l'ordine di invio.

**`successor(int self, Collection<Integer> memberIds, Set<Integer> suspected)`
→ `Optional<Integer>`** (`:49`)
Primo id non sospettato nella sequenza di hop.

**`walkFrom(int self, List<Integer> ring)`** (`:66`, privato) costruisce la
sequenza di visita: prima gli id **maggiori** di `self` in ordine crescente,
poi quelli **minori** (il wrap-around). Non si usa aritmetica modulare su
indici — fragile con id non contigui o con `self` fuori dal ring. Conseguenze
volute, tutte coperte da test:

- `self` è escluso per costruzione → **una replica non è mai successore di sé
  stessa**;
- si saltano **N sospettati consecutivi**, non uno solo;
- lo skip funziona anche attraverso il wrap-around;
- se tutti gli altri sono sospettati si ottiene `Optional.empty()` invece di un
  loop su sé stessi (caso gestito da `Replica.forwardElection:521`, che vince
  per default);
- sospettare sé stessi non cambia il successore;
- `self` non deve appartenere a `memberIds`: la visita parte dal suo punto di
  inserimento.

### 7.2 `ElectionLogic.java` (113 righe)

**`NONE = new UpdateID(0, 0)`** (`:26`) — sentinella per history vuota
(decisione D2). È strettamente minore di qualunque id reale, perché il primo
update di un epoch è `⟨e,1⟩` (`nextInEpoch` parte da `⟨e,0⟩`).

**`winner(Map<Integer,UpdateID>) → int`** (`:35`)
Massimo `UpdateID` sotto l'ordine naturale, **tie-break sull'id più alto**
(`cmp > 0 || (cmp == 0 && candidateId > winnerId)`). Rifiuta payload vuoti e
valori `null`. È una **funzione pura del solo payload**: ogni replica che vede
il giro completo calcola lo stesso vincitore senza round aggiuntivi — è ciò che
rende superfluo un secondo giro di annuncio.
Traccia §1: *"the replica that knows the most recent update; replica
identifiers are used to break ties"*.

**`latestOf(UpdateHistory) → UpdateID`** (`:66`)
`history.latestId()` oppure `NONE`. Rende impossibile infilare `null` nel
payload.

**`withEntry(Map, int replicaId, UpdateID latest) → Map`** (`:83`)
Copia difensiva + `putIfAbsent` + `unmodifiableMap`; non muta mai l'input. Il
"first writer wins" traduce alla lettera *"if it is not already participating
in the election, it adds its own information"*: se rivedo il mio id il
messaggio ha completato il giro e il payload deve restare **identico** a quello
visto da tutti gli altri, altrimenti repliche diverse potrebbero calcolare
vincitori diversi.

**`newEpoch(Map) → int`** (`:101`)
`max(epoch osservati) + 1` (decisione D3). Il massimo è preso su **tutto il
payload** e non solo sulla history del vincitore: se una replica ha visto un
epoch più alto, riusare un epoch già speso romperebbe l'unicità degli
`UpdateID`.

### 7.3 `SyncPlan.java` (72 righe)

**`missingFor(UpdateHistory winnerHistory, UpdateID recipientLatest)`** (`:30`)
Wrapper su `UpdateHistory.after(...)`, tenuto come seam esplicito perché il
livello di elezione non tocchi mai gli interni della history. Totale anche nei
casi degeneri (destinatario più avanti del vincitore → lista vuota).

**`oldest(Map<Integer,UpdateID>)`** (`:58`)
Minimo dei `latestId` del payload — il "watermark" sotto il quale ogni
partecipante è sicuramente allineato. Rifiuta payload vuoti e `null`.

**`missingForAll(UpdateHistory, Map)`** (`:48`)
`missingFor(history, oldest(payload))`. Il motivo è strutturale:
`Synchronization` è un **broadcast unico** con una sola lista, quindi il
vincitore non può spedire una lista personalizzata per destinatario. La lista
giusta è allora il diff rispetto alla **replica più indietro** fra i
partecipanti; chi ha già applicato una parte di quegli update li scarta, perché
la consegna è **idempotente sull'`UpdateID`** (property Integrity: *"a process
delivers m at most once"*).

---

## 8. `Client.java` (140 righe)

Estende `AbstractClient`. Modello mentale: *fire-and-forget con timeout per
richiesta*; il client **non ritenta mai** (ed è per questo che la replica deve
bufferizzare, vedi §13).

**Campi**

| Campo | Riga | Ruolo |
|-------|------|-------|
| `long nextReqId` | 28 | contatore monotono di richieste |
| `Set<Long> pending` | 32 | id ancora senza risposta |

**Costruzione** — costruttore package-private (`:34`) e due factory:
`props(readTimeout, writeTimeout, defaultTargetReplica)` (`:38`) per la
produzione e `propsWithListener(..., listener)` (`:43`) per i test.

**`sendRead(ActorRef replica, int index)`** (`:48`)
Genera `reqId`, lo registra in `pending`, logga
`requesting READ (<idx>) to <ReplicaID>` — il formato letterale della traccia
§2 — invia `ClientRead` e schedula un `ReadTick` su sé stesso dopo
`getReadTimeoutDelay()`.

**`sendWrite(ActorRef replica, int index, int value)`** (`:57`) — simmetrico,
con `ClientWrite` e `WriteTick`.

**`createReceive()`** (`:66`) parte da `createBaseReceiveBuilder()` (obbligo
della codebase) e aggiunge `ReadReply`, `WriteReply`, `ReadTick`, `WriteTick`.

**Handler** (`:75-98`) — tutti e quattro seguono lo stesso schema:

```java
if (pending.remove(msg.reqId)) { … }
```

**Scelta implementativa**: nessun `Cancellable` lato client. Il tick è sempre
schedulato e la mutua esclusione fra "risposta" e "timeout" è realizzata dalla
semantica di `Set.remove` — vince chi arriva primo, un tick tardivo è
silenziosamente ignorato. Meno stato, nessuna race, nessun timer da ripulire.

**Tick interni** — `ReadTick` (`:114`) e `WriteTick` (`:126`), `private static
final`, contengono `reqId`, la replica e i parametri della richiesta, così la
callback di timeout può riportare i campi esatti che i test si aspettano
(`APICompliance.crashReplicaAndTryRequests` confronta l'oggetto intero con
`equals`).

---

## 9. `Replica.java` — anatomia completa

881 righe, 46 metodi. È il cuore del progetto: implementa la macchina a stati,
il two-phase broadcast, la crash detection, l'elezione ad anello e la
sincronizzazione.

### 9.1 Campi di stato (`:54-114`)

| Campo | Riga | Ruolo |
|-------|------|-------|
| `int[] positions` | 54 | lo stato replicato, `POSITIONS_LIST_LENGTH = 100` interi a zero |
| `Map<Integer,ActorRef> group` | 57 | vista del sistema, riempita da `initSystem` |
| `int coordinatorId` | 58 | chi crediamo essere il coordinatore *adesso* |
| `UpdateHistory history` | 61 | log locale degli update deliverati |
| `UpdateID lastAssignedId` | 65 | **coordinatore**: ultimo `⟨e,i⟩` assegnato. Su una non-coordinatrice tiene l'epoch corrente (`⟨e,0⟩`), usato dalla guardia di idempotenza della `Synchronization` |
| `Map<UpdateID,Integer> ackCounts` | 67 | **coordinatore**: ack raccolti per id |
| `Set<UpdateID> committed` | 69 | **coordinatore**: id che hanno già raggiunto il quorum → niente doppio WRITEOK |
| `Map<UpdateID,UpdateMsg> pendingUpdates` | 73 | proposte di fase 1 ack-ate e in attesa del WRITEOK |
| `Map<UpdateID,Cancellable> updateTimeouts` | 77 | un `UpdateTimeout` armato per proposta |
| `Map<Long,Cancellable> forwardTimeouts` | 82 | un `ForwardTimeout` armato per richiesta inoltrata, **chiavato per `reqId`**: due richieste diverse possono scrivere lo stesso valore allo stesso indice, quindi `(index, value)` non sarebbe una chiave sicura |
| `Map<Long,PendingClientWrite> clientWrites` | 90 | write per cui questa replica è stata contattata e non ha ancora risposto. `LinkedHashMap`: l'ordine FIFO per client sopravvive a un replay |
| `Crash pendingCrash` | 93 | crash differito armato dall'esterno |
| `Map<Crash.Type,Integer> messageCounters` | 94 | contatori per tipo |
| `Cancellable heartbeatTask` | 97 | **coordinatore**: il loop periodico di beat |
| `Cancellable heartbeatTimeoutTask` | 98 | **non-coordinatore**: il watchdog sul coordinatore |
| `Set<Integer> suspected` | 104 | repliche ritenute morte: il coordinatore crashato più ogni successore che non ha ack-ato. Le repliche non si riprendono, quindi l'insieme **cresce**; l'unica rimozione è in `onSynchronization`, dove il mittente dell'annuncio dimostra di essere vivo e va tolto dalla blacklist del ring |
| `boolean participating` | 106 | vero mentre siamo nel behavior ELECTION |
| `boolean electionStartedFired` | 109 | garantisce **al massimo una** `callbackOnElectionStarted` per elezione, anche se il round riparte per `GlobalElectionTimeout` |
| `Election lastForwardedElection` | 111 | ultimo payload inoltrato, da rigiocare quando si salta un successore |
| `Cancellable electionAckTimeoutTask` | 113 | attesa dell'ack del successore |
| `Cancellable globalElectionTimeoutTask` | 114 | rete anti-livelock |

**Classi interne**

- `SendHeartbeatTick` (`:117`) — self-message che lo scheduler usa per far
  partire un giro di heartbeat *dentro* il thread dell'attore.
- `PendingClientWrite` (`:121`) — `(reqId, index, value, client)`, `private
  static final`, immutabile.

### 9.2 Costruzione e inizializzazione

| Metodo | Riga | Cosa fa |
|--------|------|---------|
| `Replica(int id)` | 135 | costruttore di comodo con i default della codebase |
| `Replica(id, minLat, maxLat, beat, listener)` | 140 | delega a `super` |
| `props(...)` | 144 | factory di produzione (nessun listener) |
| `propsWithListener(...)` | 150 | factory per i test: il listener è la `TestKit` probe |
| `getSystemNumberOfActors()` | 157 | `group.size()`, **0 prima dell'init** — perciò `getMaxLatencyPlusTolerance()` prima dell'init vale solo `maxLatency` |
| `initSystem(InitSystem)` | 191 | salva `group` e `coordinatorId`, logga, e **avvia i timer di ruolo**: `startHeartbeatLoop()` se siamo il coordinatore, altrimenti `resetHeartbeatTimeout()` |

### 9.3 I tre behaviour della FSM

Tutte le transizioni passano da `getContext().become(...)`; non si chiama mai
`getContext().stop()`.

| Behavior | Costruito in | Accetta |
|----------|--------------|---------|
| **NORMAL** | `createReceive()` `:209` | tutto il protocollo, **inclusi** `Election`/`ElectionAck`/`ElectionAckTimeout`/`GlobalElectionTimeout`/`Synchronization` — una replica che non ha ancora rilevato il crash può essere trascinata in un round, o apprenderne l'esito, restando in NORMAL |
| **ELECTION** | `election()` `:245` | `ClientRead` (le read continuano: non coinvolgono il coordinatore), `ClientWrite` (**bufferizzata**), i quattro messaggi di elezione, `Synchronization`, `Crash`; **tutto il resto è scartato** con un `debug(...)` |
| **CRASHED** | `crashed()` `:181` | niente: `matchAny` che scarta in silenzio |

Entrambi i behaviour vivi partono da `createBaseReceiveBuilder()`, così i
comandi `Crash` continuano a passare dal wrapper obbligatorio (log + notifica
al listener) anche durante un'elezione. `crashed()` invece parte da
`receiveBuilder()` nudo: se partisse dal base builder ri-gestirebbe `Crash`.

Scartare `UpdateMsg`/`WriteOk`/`Heartbeat` durante ELECTION **congela il
"latest known update" di ogni candidato per tutta la durata del round**, così
il payload dell'`Election` non può diventare inconsistente mentre gira.

Transizioni:

```
initSystem ──► NORMAL
NORMAL ──(HeartbeatTimeout | ForwardTimeout | UpdateTimeout | Election ricevuta)──► ELECTION
ELECTION ──(becomeWinner | Synchronization)──► NORMAL
qualunque stato ──(Crash)──► CRASHED   (terminale)
```

### 9.4 Percorso di lettura

**`onClientRead(ClientRead)`** (`:331`) — tre righe: legge `positions[idx]`,
logga, risponde `ReadReply(reqId, idx, value, id)` con `fromReplica = id`
(la replica **contattata**). Nessun coinvolgimento del coordinatore, nessun
timer. Traccia §1: *"The contacted replica immediately replies with the current
value stored at that index"*.

### 9.5 Percorso di scrittura (two-phase)

**Fase 0 — arrivo e inoltro**

- `onClientWrite` (`:347`) registra la richiesta in `clientWrites` e chiama
  `submitClientWrite`.
- `onClientWriteDuringElection` (`:354`) — in stato ELECTION non c'è un
  coordinatore a cui inoltrare: la write viene **parcheggiata** (decisione D4)
  e verrà rigiocata a elezione conclusa.
- `submitClientWrite` (`:360`) — se siamo il coordinatore chiama `startUpdate`
  direttamente; altrimenti invia `ForwardWrite` al coordinatore **e** arma il
  `ForwardTimeout`.
- `replayPendingClientWrites` (`:375`) — rimanda al nuovo coordinatore ogni
  write ancora senza risposta, iterando su una copia della `LinkedHashMap` per
  preservare l'ordine FIFO del client.
- `onForwardWrite` (`:385`) — ignorato da chi non è coordinatore; altrimenti
  entra in `startUpdate` con i dati di routing originali.

**Fase 1 — proposta**

- `startUpdate` (`:393`) — `lastAssignedId = lastAssignedId.nextInEpoch()`,
  crea l'`Update`, azzera il contatore di ack, logga `UPDATE proposed` e fa
  `broadcast(new UpdateMsg(...), Crash.Type.Update)`. Il broadcast include **il
  coordinatore stesso**, come richiede la traccia (*"Since the coordinator
  itself is also a replica, the quorum includes the coordinator"*).
- `onUpdateMsg` (`:402`), su ogni replica:
  1. `checkCrashCondition(Update)` — punto di crash istrumentato "after
     receiving an UPDATE";
  2. riarma l'`HeartbeatTimeout` (l'`UpdateMsg` è di per sé prova di vita del
     coordinatore: non serve aspettare il prossimo beat);
  3. cancella il `ForwardTimeout` di quel `reqId` — il coordinatore *ha*
     avviato il broadcast;
  4. memorizza la proposta in `pendingUpdates`;
  5. invia `UpdateAck` al coordinatore e arma l'`UpdateTimeout` per quell'id.

**Fase 2 — commit**

- `onUpdateAck` (`:420`) — ignorato da chi non è coordinatore e per gli id già
  committati. Accumula in `ackCounts`; al raggiungimento di
  `quorum() = ⌊N/2⌋+1` marca l'id in `committed` (idempotenza: gli ack
  successivi non rifanno il broadcast), lo toglie da `ackCounts` e fa
  `broadcast(new WriteOk(id), Crash.Type.WriteOK)`.
- `onWriteOk` (`:434`) — crash check, riarmo dell'heartbeat timeout,
  cancellazione dell'`UpdateTimeout`; se la proposta è ancora in
  `pendingUpdates` chiama `applyUpdate`, altrimenti esce (già deliverata o mai
  vista).
- **`applyUpdate(Update u)`** (`:456`) — **l'unico punto in cui lo stato
  cambia**:
  ```java
  positions[u.index] = u.value;
  history.append(u);
  log("applied update " + u.id.epoch + ":" + u.id.sequence + " (" + u.index + ", " + u.value + ")");
  callbackOnUpdateApplied(u.index, u.value);
  ```
  poi rimuove la proposta da `pendingUpdates` (una seconda consegna dello
  stesso id non riapplica nulla), cancella l'`UpdateTimeout` e — **solo se
  `contactedReplicaId == id`** — libera la `clientWrites`, cancella il
  `ForwardTimeout` e risponde `WriteReply` al client. È così che
  `WriteResult.fromReplica` risulta uguale alla replica contattata.
  Il formato di log è esattamente quello della traccia §2:
  `[Replica <id>] applied update <epoch>:<sequence> (<idx>, <val>)`.
- `alreadyDelivered(UpdateID)` (`:473`) — `uid ≤ history.latestId()`. Funziona
  perché la history è ordinata e non può avere buchi interni: il canale è FIFO
  per coppia e una replica che crasha non riprende, quindi un `WriteOk` mancato
  implica che nessun `WriteOk` successivo di quello stesso coordinatore arriverà.

### 9.6 Crash detection

- `startHeartbeatLoop` (`:262`) — solo il coordinatore. Cancella il watchdog
  (non serve più) e apre uno `scheduleWithFixedDelay(Zero,
  coordinatorBeatInterval)` che si manda `SendHeartbeatTick`; il `Receive`
  (`:217`) lo traduce in `broadcast(new Heartbeat(), Crash.Type.Heartbeat)`.
  Passare da un self-message invece di inviare direttamente dallo scheduler
  tiene **tutti** gli invii dentro il thread dell'attore.
- `rearmHeartbeatTimeout` (`:275`) — usata da chi *torna* non-coordinatore dopo
  una `Synchronization`: spegne l'eventuale loop di beat e riarma il watchdog.
- `resetHeartbeatTimeout` (`:281`) — cancella il timer precedente **prima** di
  riarmare (riassegnare la variabile senza cancellare lascerebbe vivo il
  vecchio timer, con timeout spuri) e schedula `HeartbeatTimeout` a
  `3 × coordinatorBeatInterval` = 3 s con i default.
- `onHeartbeat` (`:293`) — crash check e riarmo del watchdog.
- `onHeartbeatTimeout` (`:302`) — se siamo il coordinatore, no-op; altrimenti
  logga `HEARTBEAT TIMEOUT` e chiama `startElection(coordinatorId)`.
- `onForwardTimeout` (`:309`) — copre *"a replica that forwards a write request
  to the coordinator starts a timeout and detects a failure if the coordinator
  does not initiate the broadcast protocol in time"*. Rimuove il timer dalla
  mappa e avvia l'elezione.
- `onUpdateTimeout` (`:318`) — copre *"A replica detects that the coordinator
  has crashed if it does not receive a WRITEOK message within a predefined
  timeout after receiving an UPDATE"*. Stessa struttura.

Il fattore 3 sul beat è largamente sopra il RTT massimo (`maxLatency = 20 ms`),
quindi rispetta il requisito *"Crash detection is assumed to be accurate and
does not produce false positives"*.

### 9.7 Crashed mode e istrumentazione dei crash

- `crash(Crash how_to_crash)` (`:162`) — implementazione del metodo astratto.
  Memorizza `pendingCrash`; se il tipo è `Now` chiama subito `triggerCrash()`,
  **ignorando `after_n_messages_of_type`**: per `Now` non esiste un tipo di
  messaggio da contare, e trattare un `n > 0` come condizione differita
  significherebbe armare un contatore che nessuno incrementa mai, lasciando
  viva una replica che il chiamante crede morta.
- `triggerCrash()` (`:176`) — `cancelAllTimers()` + `become(crashed())`.
  Cancellare i timer è ciò che realizza il *"stops sending messages to other
  actors"*: senza, una replica "morta" continuerebbe a emettere heartbeat o a
  far scattare elezioni.
- `crashed()` (`:181`) — `matchAny` che scarta, con commento esplicito sul
  fatto che **non** si chiama `getContext().stop()`, come impone la traccia §2.
- `checkCrashCondition(Crash.Type)` (`:932`) — la semantica *"processa N
  messaggi di quel tipo, poi crasha"*:
  ```java
  if (pendingCrash != null && pendingCrash.type == type) {
      int currentCount = messageCounters.getOrDefault(type, 0);
      if (currentCount >= pendingCrash.after_n_messages_of_type) { triggerCrash(); return false; }
      messageCounters.put(type, currentCount + 1);
  }
  return true;
  ```
  Con `after_n = 2`: primo e secondo messaggio processati (contatore 0→1→2), al
  terzo `2 >= 2` e la replica crasha **senza** processarlo. Con `after_n = 0`
  crasha sul primo.

**I due lati del contatore.** La condizione è valutata in due posti diversi, ed
è questo che copre tutti i punti di crash chiesti dalla traccia §2 (*"during
the broadcast of an UPDATE, after receiving an UPDATE, during the dissemination
of WRITEOK messages, or while the coordinator election is in progress"*):

- **in ricezione**, all'inizio degli handler di `UpdateMsg` (`:404`), `WriteOk`
  (`:436`) e `Heartbeat` (`:294`) — attraverso
  `checkIncomingCrashCondition` (`:911`) — e di `Election` (`:530`), che
  chiama direttamente `checkCrashCondition` non passando mai da un broadcast;
- **in invio**, dentro `broadcast(msg, crashPoint)` (`:823`), che valuta la
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

| Comando | Sul coordinatore | Su una non-coordinatrice |
|---------|------------------|--------------------------|
| `Crash(Update, n)` | muore dopo aver inviato l'UPDATE a `n` repliche | muore dopo aver processato `n` UPDATE |
| `Crash(WriteOK, n)` | muore dopo aver inviato il WRITEOK a `n` repliche | muore dopo `n` WRITEOK |
| `Crash(Heartbeat, n)` | muore dopo `n` heartbeat inviati | muore dopo `n` heartbeat ricevuti |
| `Crash(Election, n)` | — | muore dopo `n` messaggi ELECTION processati |
| `Crash(Now, 0)` | muore subito | muore subito |

**Un messaggio, un incremento.** Il contatore per tipo è uno solo, ma il
coordinatore compare **anche fra i destinatari dei propri broadcast**: senza
precauzioni conterebbe due volte lo stesso UPDATE / WRITEOK / HEARTBEAT, una in
uscita e una alla consegna, falsando la semantica
`after_n_messages_of_type`. Per questo gli handler in ricezione passano da
`checkIncomingCrashCondition` (`:911`), che è un no-op quando siamo noi ad aver
fatto quel broadcast:

```java
private boolean checkIncomingCrashCondition(AbstractReplica.Crash.Type type) {
    return isCoordinator() || checkCrashCondition(type);
}
```

La regola è quindi: **per un tipo che viaggia in broadcast, chi lo manda conta
in uscita e tutti gli altri contano in entrata**. `Election` non passa mai da
`broadcast`, quindi il suo handler chiama direttamente `checkCrashCondition`.

**È questa la parte che rende dimostrabile la "partial dissemination"**: con
`Crash(WriteOK, 2)` il coordinatore consegna il WRITEOK a due repliche soltanto
e muore — lo scenario della Demo 4 e del test
`CornerCases.coordinatorCrashesDuringWriteOkDissemination`.

Due dettagli deliberati:

- il ciclo scorre le repliche in **ordine crescente di id**
  (`RingTopology.order`) e non sui `values()` della `HashMap`: l'insieme delle
  repliche servite prima del crash è così riproducibile fra un'esecuzione e
  l'altra, che è quello che serve a una demo e a un test deterministico;
- la `Synchronization` **non** passa da `broadcast` (è un ciclo a parte in
  `becomeWinner`, perché deve escludere il vincitore): il crash "durante
  l'elezione" resta quello sul messaggio `Election`, che è il punto citato
  dalla traccia.

### 9.8 Elezione

**`startElection(int crashedCoordinatorId)`** (`:485`)
1. guardia di idempotenza (`participating || group == null` → return);
2. `participating = true`, `suspected.add(crashedCoordinatorId)`;
3. **cancella l'`HeartbeatTimeout`**: mentre siamo candidati non dobbiamo
   sospettare nessun altro per assenza di heartbeat;
4. `become(election())`, `fireElectionStartedOnce(...)`,
   `rearmGlobalElectionTimeout()`;
5. costruisce il payload con la sola entry propria
   (`withEntry(emptyMap, id, latestOf(history))`) e lo inoltra al successore.

**`fireElectionStartedOnce(int)`** (`:506`) — guardia su `electionStartedFired`:
al massimo una `callbackOnElectionStarted` per partecipazione, anche se il
round viene riavviato. È il requisito verificato da
`APICompliance.callbackOnElectionStartedCalledAtMostOncePerReplica`.

**`forwardElection(Election)`** (`:514`)
Memorizza il payload in `lastForwardedElection`, chiede il successore a
`RingTopology.successor(id, group.keySet(), suspected)`. Se c'è, invia e arma
`ElectionAckTimeout(successorId)`. Se non c'è (**siamo gli unici vivi**), logga
e vince per default.

**`onElection(Election)`** (`:528`) — il cuore del round:
1. `checkCrashCondition(Election)` — punto di crash "while the coordinator
   election is in progress"; se scatta, la replica muore **senza ack-are** e il
   mittente la salterà per timeout;
2. **ack immediato al mittente** (`:534`), prima di qualunque altra logica: chi
   ha inoltrato deve poter cancellare il proprio `ElectionAckTimeout` al più
   presto;
3. `isStaleElection` (`:598`) — scarta un `Election` ritardatario di un round
   già deciso, cioè uno che eleggerebbe il coordinatore che stiamo già
   seguendo; se siamo **noi** il coordinatore rispondiamo con una
   `Synchronization` mirata, così il mittente rimasto indietro esce dal round
   invece di restarci bloccato;
4. se non stiamo già partecipando → entriamo in ELECTION con lo stesso setup di
   `startElection`, e `fireElectionStartedOnce(coordinatorId)`: il coordinatore
   in cui credevamo è quello crashato;
5. **se il payload contiene già il nostro id** → il giro è completo e il
   payload è definitivo: calcoliamo `winner(...)`. Se siamo noi →
   `becomeWinner`; altrimenti inoltriamo **così com'è** (senza toccare il
   payload), perché il messaggio deve **raggiungere il vincitore**, unico
   autorizzato ad annunciare l'esito;
6. altrimenti → inoltriamo `withEntry(payload, id, latestOf(history))`.

**`isStaleElection(Election)`** (`:598`) — riconosce un round già deciso.
Precondizioni: non stiamo partecipando e non sospettiamo il coordinatore che
seguiamo. Poi due segnali indipendenti:

1. **siamo noi quel coordinatore.** La traccia assume una crash detection
   accurata, quindi nessuna replica corretta può stare correndo un round
   *contro* un coordinatore palesemente vivo: questo messaggio appartiene a un
   round su un crash precedente, già risolto dalla nostra stessa elezione.
   Riconoscerlo qui è necessario perché il payload di un round **riavviato**
   non porta ancora la nostra entry, quindi il vincitore calcolato su di esso
   sarebbe un altro e il segnale 2 mancherebbe il caso, trascinando un
   coordinatore sano in un'elezione superflua e bruciando un epoch;
2. **il round, così com'è, eleggerebbe il coordinatore che già seguiamo.**

In entrambi i casi, se siamo il coordinatore rispondiamo al mittente con una
`Synchronization` mirata, il cui payload è calcolato da **`syncPayloadFor`**
(`:616`): il diff rispetto al partecipante più indietro del round, o l'intera
history se l'`Election` non porta alcuna entry (caso degenere: non sappiamo
nulla del mittente, e la consegna è comunque idempotente sull'`UpdateID`).

**`onElectionAck` / `onElectionAckTimeout`** (`:623`, `:628`)
L'ack cancella il timer. Il timeout aggiunge il successore a `suspected`, logga
`ELECTION ACK TIMEOUT: suspecting k, skipping it in the ring` e **rigioca
`lastForwardedElection`** — `RingTopology.successor` calcola già il prossimo
saltando tutti i sospettati. Traccia §1: *"the sender assumes that the next
replica in the ring has crashed, skips it, and forwards the message to the
following replica"*.

**`onGlobalElectionTimeout`** (`:645`) — rete anti-livelock. Azzera lo stato
parziale del tentativo, mette `participating = false` e richiama
`startElection(coordinatorId)`. La callback **non** viene rifatta
(`electionStartedFired` resta `true`): è ancora la stessa partecipazione, solo
un altro tentativo. È la rete di sicurezza per il caso in cui il vincitore
crashi prima di annunciarsi: al riavvio, il primo inoltro verso il vincitore
morto scade sull'`ElectionAckTimeout`, che lo aggiunge a `suspected`, e il
round prosegue senza di lui. Testato da
`CornerCases.electionWinnerCrashesBeforeSynchronization`.

**`rearmElectionAckTimeout(int)`** (`:656`) — one-shot a
`getMaxLatencyPlusTolerance()`.
**`rearmGlobalElectionTimeout()`** (`:661`) — one-shot a
`N × getMaxLatencyPlusTolerance() × 2`, abbastanza per un giro completo del
ring.

### 9.9 Sincronizzazione e update orfani

**`becomeWinner(Map<Integer,UpdateID> latestPerReplica)`** (`:679`) —
**l'ordine delle fasi è la parte critica**:

```
1. cancella ElectionAckTimeout e GlobalElectionTimeout, azzera lo stato di elezione
2. newEpoch = ElectionLogic.newEpoch(payload)
3. completeInterruptedUpdates()            ← PRIMA di aprire il nuovo epoch
4. coordinatorId = id;  lastAssignedId = ⟨newEpoch, 0⟩;  azzera ackCounts/committed
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

**`completeInterruptedUpdates()`** (`:722`) — itera i `pendingUpdates`
**ordinati per `UpdateID`** (`Collections.sort` sulle chiavi: l'ordine totale
va preservato anche in recovery), salta quelli già deliverati e applica gli
altri, poi svuota la mappa. Sono le proposte che la replica aveva ack-ato ma
mai visto confermare: il vecchio coordinatore può essere morto dopo averle
applicate, o dopo aver mandato il WRITEOK a qualcun altro.

**`onSynchronization(Synchronization)`** (`:739`) — lato ricevente.

Prima di tutto una **guardia di idempotenza**: se non stiamo partecipando a
un'elezione e l'annuncio riguarda il coordinatore e l'epoch che abbiamo già
adottato, il messaggio è un duplicato e viene ignorato. Serve davvero: il nuovo
coordinatore risponde con una `Synchronization` a **ogni** `Election`
ritardataria che gli arriva (vedi `isStaleElection`), quindi la stessa replica
può ricevere lo stesso annuncio più volte. Rieseguire l'handler rigiocherebbe
le write bufferizzate una seconda volta, trasformando **una** richiesta del
client in **due** update distinti.

Superata la guardia: cancella i timer di elezione, azzera lo stato di
partecipazione, **toglie il nuovo coordinatore da `suspected`** — questo
messaggio è prova che è vivo, e una replica trascinata in un round su un
coordinatore già sostituito avrebbe sospettato proprio lui — adotta il nuovo
`coordinatorId`, poi

- **replay filtrato**: `if (!alreadyDelivered(u.id)) applyUpdate(u)` — il
  filtro di idempotenza è ciò che impedisce una seconda
  `callbackOnUpdateApplied` per la stessa write;
- scarta tutto ciò che resta dell'epoch morto (`updateTimeouts` cancellati e
  svuotati, `pendingUpdates`, `ackCounts`, `committed`): da qui in poi la sola
  sorgente di verità è il nuovo coordinatore;
- `lastAssignedId = ⟨newEpoch, 0⟩`;
- torna in NORMAL, riarma l'`HeartbeatTimeout` (`rearmHeartbeatTimeout`),
  chiama `callbackOnCoordinatorElected` e rigioca le write bufferizzate.

**Write che sopravvivono all'elezione.** Il client emette ogni richiesta **una
sola volta** e non ritenta: una write il cui coordinatore muore a metà andrebbe
persa per sempre. Perciò `clientWrites` trattiene ogni write per cui questa
replica è stata contattata e non ha ancora risposto; durante ELECTION le nuove
write sono **parcheggiate**, non scartate; `replayPendingClientWrites` le
rimanda al nuovo coordinatore sia sul vincitore sia su chi riceve la
`Synchronization`; l'entry è rimossa solo in `applyUpdate`, quando il client
riceve davvero il suo `WriteReply`.

### 9.10 Helper

| Metodo | Riga | Note |
|--------|------|------|
| `isCoordinator()` | 797 | `id == coordinatorId` |
| `quorum()` | 802 | `group.size() / 2 + 1` = `⌊N/2⌋+1` |
| `broadcast(msg, crashPoint)` | 823 | vedi §9.7 |
| `cancelTimeout(Cancellable)` | 834 | null-safe e idempotente |
| `cancelAllTimers()` | 841 | usato solo da `triggerCrash`: spegne beat, watchdog, i due timer di elezione e **tutti** i timer per-update e per-forward |
| `scheduleForwardTimeout(reqId, idx, val)` | 866 | cancella il precedente per lo stesso `reqId`, poi arma a `3 × beat` |
| `scheduleUpdateTimeout(UpdateID)` | 876 | idem, per `UpdateID` |
| `scheduleSelf(msg, delayMillis)` | 883 | wrapper su `scheduler().scheduleOnce(..., getSelf(), ...)`. È il **solo** modo in cui la replica si manda messaggi: mai `getSelf().tell(...)` |
| `checkIncomingCrashCondition(type)` | 911 | guardia lato ricezione: il coordinatore non riconteggia i propri broadcast, vedi §9.7 |
| `checkCrashCondition(type)` | 932 | vedi §9.7 |

---

## 10. `Main.java` (255 righe)

Punto d'ingresso della demo (`mainClass` in `build.gradle`). Non fa parte del
protocollo: costruisce, guida dall'esterno e smonta un sistema per ciascuno dei
quattro scenari.

**Costanti** (`:28-38`) — `N_REPLICAS = 5`, `COORDINATOR_ID = 0`,
`READ_TIMEOUT = 8 × MAX_LATENCY × N` (un paio di round trip: la read è servita
subito), `WRITE_TIMEOUT = 20 s` (una write può dover sopravvivere alla morte del
coordinatore: detection + elezione + replay).

**`main(String[] args)`** (`:40`) — imposta il logger su stdout, poi esegue lo
scenario indicato da `args[0]` (`1`…`4`) o tutti e quattro con `all`/nessun
argomento.

**Scenari** — `happyPath` (`:78`), `nonCoordinatorCrash` (`:105`),
`coordinatorCrash` (`:136`), `partialWriteOk` (`:168`). Descritti in
[§19](#19-demo-eseguibili).

**Helper** — `Cluster` (record interno `system` + `replicas`, `:196`),
`bootstrap(name, n, coordId)` (`:207`, crea le repliche e invia l'`InitSystem`
a tutte), `clientOn(cluster, name, replicaId)` (`:228`), `shutdown` (`:234`,
termina l'`ActorSystem` e attende fino a 10 s), `pause` (`:241`), `banner`
(`:245`) e `note` (`:252`, commento dello scenario con prefisso `[demo]`, così
si distingue a colpo d'occhio dai log degli attori).

**Non c'è un solo `System.out.println`**: anche i commenti della demo passano
da `Logger`.

---

## 11. I file di test

### 11.1 `TestsCommons.java` (codebase, 125 righe)

Fabbrica dei sistemi di test e **unico posto dove vivono le costanti di
timing**. `createTestSystem(name, n, coordinator[, minLat, maxLat])` (`:59`)
crea `n` repliche ognuna con la propria `TestKit` probe come listener, invia
l'`InitSystem` a tutte e disabilita il logger se `DO_PRINTS == false`.

Budget temporali che i nostri test riusano:
`getLatencyPlusEpsilon` (`:94`), `getMaxUpdateDelay` (`:99`, un giro 2PC = 6
hop + jitter), `getElectionMaxDelay` (`:109`, detection + giro del ring, ×5),
`getClientReadTimeout` / `getClientWriteTimeout` (`:116`, `:120`).

> Il commento in testa avverte: *"ALL TIMINGS ON THESE CLASS MAY BE CHANGED"* —
> l'implementazione deve reggere anche con valori diversi.

### 11.2 `base/APICompliance.java` (codebase, 25 casi)

Verifica la conformità alle API e alle callback obbligatorie:
`oneClientWriteWaitRead` (4 parametrizzazioni), `replicasCrashNow`,
`crashReplicaAndTryRequests` (2), `callbackOnUpdateAppliedInvokedOnAllReplicas`
(4), `callbackOnUpdateAppliedOncePerWrite` (2),
`callbackOnElectionStartedInvokedCorrectly` (4),
`callbackOnElectionStartedCalledAtMostOncePerReplica`,
`callbackOnCoordinatorElectedAllAgree` (4),
`callbackOnCoordinatorElectedNewCoordAlsoCalls` (3).

### 11.3 `base/NoCrashes.java` (codebase, 4 casi)

`sequentialConsistencyOneWriteClient` per `(coordinator, N) ∈ {(0,7), (0,22),
(1,7), (1,22)}`: un client scrive 5 valori crescenti su una replica mentre
**un client di lettura per ogni replica** fa 15 read in parallelo su thread
separati. Asserisce che la sequenza di valori letta da ciascuna replica sia
**non decrescente** — cioè la consistenza sequenziale della traccia §1.

### 11.4 `base/WithCrashes.java` (codebase, 4 casi)

- `nonCoordinatorsCrashClientWritesWaitsReads` (N ∈ {7, 22}): crashano
  `N/2 − 2` repliche non coordinatrici; la write deve comunque completare e la
  read successiva leggere il valore.
- `coordinatorCrashClientWritesWaitsReads` ((1,7) e (0,22)): crashano il
  coordinatore **e altre due repliche**; la write deve completare *dopo*
  l'elezione, con `fromReplica` = replica contattata.

### 11.5 `election/*Test.java` (nostri, 60 casi)

Unit test puri, senza `ActorSystem`: girano in millisecondi e sono
deterministici.

- **`RingTopologyTest`** (22) — ordinamento, stabilità, deduplicazione,
  `keySet()` di una mappa, ring vuoto, immutabilità dello snapshot, rifiuto dei
  null; successore in ordine, wrap-around, id non contigui, **giro completo che
  visita ogni replica esattamente una volta**, mai sé stessi, ring da un solo
  membro, skip di uno / due sospettati consecutivi / attraverso il wrap-around,
  tutti sospettati → `empty`, sospettare sé stessi o un id esterno è innocuo,
  `self` fuori dal ring.
- **`ElectionLogicTest`** (22) — vincitore unico, più aggiornato, epoch che
  batte la sequence, tie-break sull'id più alto, **tie-break applicato solo fra
  i più aggiornati**, history vuote ovunque, qualunque update batte una history
  vuota, **indipendenza dall'ordine di iterazione della mappa**, payload vuoto
  e `null` rifiutati; sentinella `NONE` più piccola di ogni id reale;
  `withEntry` che aggiunge, che non sovrascrive, che non muta l'input e che
  ritorna una mappa non modificabile; `newEpoch` = max+1, prima elezione su
  history vuote → epoch 1, **max preso su tutti i partecipanti e non solo sul
  vincitore**.
- **`SyncPlanTest`** (16) — history vuota del destinatario → tutto il log,
  replica indietro → solo ciò che le manca, replica allineata o più avanti →
  niente, ordine totale preservato, **diff che attraversa il confine di epoch**,
  history vuota del vincitore, snapshot immodificabile, `oldest` come minimo e
  confronto epoch-prima-di-sequence, broadcast che copre il più indietro,
  broadcast vuoto se tutti allineati, rifiuto di payload vuoti e null.

### 11.6 `scenarios/CornerCases.java` (nostro, 5 casi end-to-end)

Copre i corner case del fault model dello Sprint 4. Ogni scenario mantiene viva
una maggioranza stretta, come impone il modello.

| Test | Innesco | Cosa dimostra |
|------|---------|---------------|
| `coordinatorCrashesDuringUpdateBroadcast` | `Crash(Update, 2)` sul coordinatore, N=5 | la proposta non raggiunge il quorum e viene giustamente scartata (nessun client si è mai sentito dire "ok"), ma **la richiesta del client non si perde**: viene bufferizzata, rigiocata nel nuovo epoch, e tutte le repliche vive convergono |
| `coordinatorCrashesDuringWriteOkDissemination` | `Crash(WriteOK, 2)` | **uniform agreement**: una sola replica ha applicato l'update, vince l'elezione proprio per questo, e la sua `SYNCHRONIZATION` lo rimette in circolo |
| `twoConsecutiveReplicasCrashDuringElection` | `Crash(Now,0)` su 2 e 3, poi sul coordinatore, N=7 | il ring sopravvive a un **buco di due nodi adiacenti**: chi inoltra a 2 va in timeout due volte di fila prima di trovare un vivo. Poi il sistema torna scrivibile |
| `electionWinnerCrashesBeforeSynchronization` | `Crash(Election, 1)` sul futuro vincitore + `Crash(Now,0)` sul coordinatore | solo il `GlobalElectionTimeout` può sbloccare il sistema: si verifica che un secondo round parta e converga su una replica viva |
| `replicaCrashesAfterAckBeforeApplying` | `Crash(WriteOK, 0)` su una non-coordinatrice | il quorum non dipende da una singola replica: la write completa, tutti gli altri convergono, e **nessuna elezione viene avviata** (il coordinatore è vivo) |

Helper degni di nota: `armCrash` (`:255`) attende la conferma del comando dalla
probe prima di procedere, così il crash è armato *prima* che il protocollo si
muova; `assertConverged` (`:339`) legge l'indice **direttamente da ogni
replica** (le read sono locali) invece di fidarsi di ciò che il protocollo
dichiara; `assertNoElectionStarted` (`:291`) usa `fishForMessage` per drenare
gli `UpdateApplied` e fallire solo se compare un `ElectionStarted`.

---

# Parte III — Il protocollo in azione

## 12. Percorso di una READ

Traccia §1: *"The contacted replica immediately replies with the current value
stored at that index"*.

```
Client                         Replica k
  │ sendRead: reqId=n            │
  │ pending.add(n)               │
  │ schedule(ReadTick(n))        │
  ├── ClientRead(n, idx) ───────►│  onClientRead
  │                              │  value = positions[idx]
  │◄── ReadReply(n, idx, v, k) ──┤  (via NetworkChannel: FIFO + ritardo)
  │ pending.remove(n) == true    │
  │ callbackOnReadResult(...)    │
```

1. `Client.sendRead` (`Client.java:48`) genera un `reqId`, lo mette in
   `pending`, logga `requesting READ (<idx>) to <ReplicaID>`, invia `ClientRead`
   e arma un tick di timeout su sé stesso.
2. `Replica.onClientRead` (`Replica.java:331`) legge `positions[idx]` e
   risponde `ReadReply(reqId, idx, value, id)` — `fromReplica` è **la replica
   contattata**.
3. `Client.onReadReply` (`Client.java:75`) rimuove il `reqId` da `pending` e,
   solo se era ancora pendente, invoca `callbackOnReadResult`.
4. Se il tick scatta prima della risposta, `onReadTick` trova il `reqId` ancora
   pendente e invoca `callbackOnReadTimeout`.

Le read sono servite **anche durante un'elezione** (`Replica.java:247`): non
coinvolgono il coordinatore, quindi non c'è motivo di bloccarle. La conseguenza
è che una read durante l'elezione può restituire un valore che non include un
update ancora in volo — coerente con la **consistenza sequenziale** richiesta
dalla traccia (*"sequential consistency from each replica's point of view"*),
non con la linearizzabilità.

---

## 13. Percorso di una WRITE (two-phase)

```
Client        Replica k (contattata)      Coordinatore c        Tutte le repliche
  │ ClientWrite(n,i,v)  │                       │                      │
  ├────────────────────►│ clientWrites[n]=…     │                      │
  │                     ├── ForwardWrite ──────►│ startUpdate          │
  │                     │  + ForwardTimeout     │ id = lastAssigned.nextInEpoch()
  │                     │                       ├── UpdateMsg(id,…) ──►│  FASE 1
  │                     │◄──────────────────────┤ (broadcast, sé incluso)
  │                     │ cancel ForwardTimeout │                      │
  │                     │ pendingUpdates[id]=…  │                      │
  │                     ├── UpdateAck(id) ─────►│◄─── UpdateAck(id) ───┤
  │                     │  + UpdateTimeout      │ ackCounts[id]++      │
  │                     │                       │ if ≥ ⌊N/2⌋+1:        │
  │                     │                       ├── WriteOk(id) ──────►│  FASE 2
  │                     │◄──────────────────────┤                      │
  │                     │ applyUpdate:          │                      │ applyUpdate
  │                     │  positions[i]=v       │                      │
  │                     │  history.append       │                      │
  │                     │  callbackOnUpdateApplied                     │
  │◄── WriteReply ──────┤ (solo la contattata)  │                      │
```

Il dettaglio dei singoli handler è in [§9.5](#95-percorso-di-scrittura-two-phase).
Tre punti che vale la pena tenere a mente:

- il broadcast di fase 1 **include il coordinatore**, che quindi ack-a sé
  stesso: il quorum lo comprende, come vuole la traccia;
- `applyUpdate` è **l'unico** punto in cui `positions[]` cambia e l'**unico**
  punto in cui `callbackOnUpdateApplied` viene invocata — è ciò che rende
  banale garantire "una volta per write";
- risponde al client **solo** la replica con `contactedReplicaId == id`.

---

## 14. Ciclo di vita di un'elezione

```
   ┌─ HeartbeatTimeout / ForwardTimeout / UpdateTimeout ─┐
   │                                                     ▼
NORMAL ──── riceve Election (round altrui) ────────►  ELECTION
   ▲                                                     │
   │                                          ┌──────────┴──────────┐
   │                                          │                     │
   │                              payload contiene il mio id?       │ no
   │                                          │ sì                  ▼
   │                                          ▼            withEntry(…) → successore
   │                                    winner(payload)              │
   │                                     │        │                  │  ack? ──no──► suspected += succ
   │                                 io  │        │ altri            │              rigioca al successivo
   │                                     ▼        ▼                  │
   │                              becomeWinner   forward as-is ──────┘
   │                                     │
   │  ◄── Synchronization ───────────────┘
   │      (onSynchronization)
   │
   └──── GlobalElectionTimeout ──► riparte da zero (senza rifare la callback)
```

**Innesco.** Tre rilevatori indipendenti portano a `startElection`:
`HeartbeatTimeout` (silenzio del coordinatore), `ForwardTimeout` (write
inoltrata e broadcast mai partito), `UpdateTimeout` (UPDATE ack-ato e WRITEOK
mai arrivato). Un quarto ingresso è passivo: ricevere una `Election` da un
vicino.

**Circolazione.** Il ring è l'ordine crescente degli id; il payload accumula
`id → latestId`. Il primo giro raccoglie le informazioni; quando il messaggio
torna a chi lo ha originato, il payload è completo e **ogni** replica che lo
vede calcola lo stesso vincitore. Il messaggio continua a girare, immutato,
finché non raggiunge il vincitore — l'unico autorizzato ad annunciare l'esito.

**Tolleranza ai guasti durante il round.** Ogni hop è ack-ato; un successore
silenzioso viene aggiunto a `suspected` e saltato (anche più d'uno di fila, e
anche attraverso il wrap-around). Se il vincitore stesso muore prima di
annunciarsi, nessuno lo saprebbe mai: il `GlobalElectionTimeout` fa ripartire
il round da zero, e questa volta il morto viene saltato.

**Conclusione.** Il vincitore completa gli update orfani, apre il nuovo epoch e
broadcasta `Synchronization`; chi la riceve applica il diff, adotta il nuovo
coordinatore e rigioca le proprie write in sospeso. Dettagli in
[§9.9](#99-sincronizzazione-e-update-orfani).

---

## 15. Come sono garantite le proprietà di safety

Le quattro proprietà del riquadro "Background: total order broadcast" della
traccia.

**Validity** — *se il mittente è corretto, prima o poi consegna m*. Una write
di un client corretto raggiunge il coordinatore; se il coordinatore muore prima
del commit, `ForwardTimeout`/`UpdateTimeout`/`HeartbeatTimeout` producono
un'elezione e `replayPendingClientWrites` rimette la richiesta in circolo verso
il nuovo coordinatore. Verificato end-to-end da
`CornerCases.coordinatorCrashesDuringUpdateBroadcast` e da
`WithCrashes.coordinatorCrashClientWritesWaitsReads`.

**Integrity** — *consegna al più una volta*. Tre filtri indipendenti:
`pendingUpdates.remove` in `applyUpdate` (fase 2 normale), il check
`alreadyDelivered` nel replay della `Synchronization`, e lo stesso check in
`completeInterruptedUpdates`. `callbackOnUpdateApplied` è invocata esattamente
in un punto del codice (`Replica.java:461`), dentro `applyUpdate`. Sul versante
della richiesta del client, la guardia sui duplicati di `onSynchronization`
impedisce che una sola write venga rigiocata due volte. Verificato da
`APICompliance.callbackOnUpdateAppliedOncePerWrite`.

**Uniform Agreement** — *se una replica consegna m, tutte le repliche corrette
consegnano m*. È il caso critico "coordinatore morto dopo aver mandato WRITEOK
solo ad alcuni". Chi ha applicato l'update ha `latestId ≥ ⟨e,i⟩`; il vincitore
dell'elezione è per costruzione la replica con il `latestId` massimo, quindi
**ha necessariamente quell'update in history** (l'assunzione della traccia — un
quorum resta sempre vivo — garantisce che almeno una replica corretta conosca
l'update più recente). `missingForAll` lo include nel broadcast, e ogni replica
indietro lo applica. Il ramo simmetrico — il coordinatore muore dopo aver
ricevuto il quorum di ack ma prima di mandare WRITEOK — è coperto da
`completeInterruptedUpdates`. Verificato da
`CornerCases.coordinatorCrashesDuringWriteOkDissemination`, che asserisce la
convergenza leggendo **da ogni replica sopravvissuta**.

**Total Order** — gli `UpdateID` sono assegnati da un solo coordinatore per
epoch (`lastAssignedId.nextInEpoch()`), gli epoch sono strettamente crescenti
(`newEpoch = max(visti)+1` su tutto il payload), la history è append-only e il
replay è ordinato (`after()` preserva l'ordine, `completeInterruptedUpdates`
ordina le chiavi). Nessuna replica può quindi applicare `w'` prima di `w` se
`id(w) < id(w')`. Verificato indirettamente da
`NoCrashes.sequentialConsistencyOneWriteClient`, che pretende sequenze di
letture non decrescenti su ogni replica.

**Perché la history non può avere buchi interni** (invariante su cui poggia
`alreadyDelivered`): il canale è FIFO per coppia e le repliche non si
riprendono, quindi se un `WriteOk` non è arrivato da un certo coordinatore,
nessun `WriteOk` **successivo** di quello stesso coordinatore arriverà. I
"buchi" possono esistere solo in coda, ed è esattamente ciò che la
`Synchronization` riempie.

---

## 16. Tabella riassuntiva dei timer

| Timer | Valore | Armato in | Cancellato da |
|-------|--------|-----------|---------------|
| heartbeat del coordinatore | `coordinatorBeatInterval` (1000 ms), periodico | `startHeartbeatLoop` `:262` | `cancelAllTimers`, `rearmHeartbeatTimeout` |
| `HeartbeatTimeout` | `3 × 1000 = 3000 ms` | `resetHeartbeatTimeout` `:281` | ogni `Heartbeat`/`UpdateMsg`/`WriteOk`; ingresso in ELECTION |
| `ForwardTimeout` | `3 × 1000 = 3000 ms` | `scheduleForwardTimeout` `:866` | arrivo dell'`UpdateMsg` con quel `reqId`; `applyUpdate` |
| `UpdateTimeout` | `3 × 1000 = 3000 ms` | `scheduleUpdateTimeout` `:876` | `WriteOk` con quell'id; `applyUpdate`; `Synchronization` |
| `ElectionAckTimeout` | `getMaxLatencyPlusTolerance()` = `20 + 10·N` ms | `rearmElectionAckTimeout` `:656` | `ElectionAck`, `becomeWinner`, `onSynchronization` |
| `GlobalElectionTimeout` | `N × getMaxLatencyPlusTolerance() × 2` | `rearmGlobalElectionTimeout` `:661` | `becomeWinner`, `onSynchronization` |
| `ReadTick` / `WriteTick` (client) | `readTimeoutDelay` / `writeTimeoutDelay` | `Client.schedule` `:101` | mai cancellati: neutralizzati da `pending.remove` |

Con `N = 7`: ack-timeout ≈ 90 ms, global ≈ 1,26 s, detection ≈ 3 s.
Il budget dei test (`TestsCommons.getElectionMaxDelay`) è
`(3000 + 2·max_lat·N + 2·N·max_lat) × 5` ≈ 17 s per N=7: ampio margine, ed è
il motivo per cui la suite non è flaky (98/98 su due run consecutivi completi).

**Perché niente falsi positivi**: la traccia assume una detection accurata. Con
`maxLatency = 20 ms` un RTT è ≤ 40 ms, mentre la finestra di detection è 3 s —
75 volte tanto. Il timeout più aggressivo è l'`ElectionAckTimeout`
(≈ 90 ms per N=7), ma un suo falso positivo costa solo un hop in più nel ring,
non un'elezione spuria.

---

## 17. Logging

Tutto passa da `Logger` (timestamp a precisione ms). **Non c'è un solo
`System.out.println` nel codice di produzione**, nemmeno in `Main`: la traccia
avverte che le stampe possono interferire con i test automatici. Le uniche
occorrenze di `System.out`/`System.err` sono dentro `Logger` stesso (lo stream
di destinazione e la diagnostica di errore del writer) e in `NoCrashes.java`,
che è file di codebase.

Formati prescritti dalla traccia §2 e dove sono prodotti:

| Formato richiesto | Dove |
|-------------------|------|
| `[Client X] requesting READ (<idx>) to <ReplicaID>` | `Client.java:51` |
| `[Client X] requesting WRITE (<idx>, <val>) to <ReplicaID>` | `Client.java:60` |
| `[Client X] READ complete (...)` / `WRITE complete (...)` | `AbstractClient:164`, `:174` (codebase) |
| `[Client X] TIMEOUT READ/WRITE request to ...` | `AbstractClient:182`, `:190` (codebase) |
| `[Replica id] applied update <epoch>:<seq> (<idx>, <val>)` | `Replica.java:459` |
| `[Replica id] CRASHED` | `AbstractReplica:372` (codebase) |

In più, log nostri sui passaggi chiave del protocollo: `initialised`,
`UPDATE proposed`, `ACK <id> to coordinator`,
`quorum reached for <id> -> WRITEOK`, `crashed while broadcasting …`,
`HEARTBEAT TIMEOUT`, `FORWARD TIMEOUT`, `UPDATE TIMEOUT`,
`ELECTION ACK TIMEOUT: suspecting k`, `GLOBAL ELECTION TIMEOUT`,
`ring lap complete, winner is k`, `WON the election`,
`SYNCHRONIZATION from k: epoch e, n update(s) to replay`,
`replaying n buffered write(s)`.

Il commento degli scenari di demo esce con il prefisso `[demo]`, così si
distingue a colpo d'occhio dai log degli attori.

---

# Parte IV — Verifica

## 18. Test: cosa esiste e cosa misura

**98 test, tutti verdi**, verificati su due esecuzioni complete consecutive
(`./gradlew build` + `./gradlew test --rerun-tasks`), senza flakiness.

| Suite | Origine | Casi | Tempo | Cosa verifica |
|-------|---------|------|-------|---------------|
| `base/APICompliance` | codebase | 25 | ~83 s | conformità alle API e alle callback obbligatorie |
| `base/NoCrashes` | codebase | 4 | ~11 s | consistenza sequenziale con N ∈ {7, 22} |
| `base/WithCrashes` | codebase | 4 | ~11 s | crash di più repliche e del coordinatore |
| `election/RingTopologyTest` | nostro | 22 | 46 ms | ring, successore, skip, wrap-around |
| `election/ElectionLogicTest` | nostro | 22 | 51 ms | vincitore, tie-break, payload, newEpoch |
| `election/SyncPlanTest` | nostro | 16 | 32 ms | diff, watermark, immutabilità |
| `scenarios/CornerCases` | nostro | 5 | ~18 s | corner case del fault model (Sprint 4) |

Il contenuto di ciascuna suite è descritto in [§11](#11-i-file-di-test).

**Copertura dei corner case della traccia §2** (*"trigger crashes at specific
points in the protocol execution"*) — tutti coperti:

| # | Caso | Innesco | Copertura |
|---|------|---------|-----------|
| 1 | Coordinatore crasha **durante il broadcast di UPDATE** | `Crash(Update, 2)` | ✅ `CornerCases.coordinatorCrashesDuringUpdateBroadcast` |
| 2 | Coordinatore crasha **dopo WRITEOK ad alcuni** (uniform agreement) | `Crash(WriteOK, 2)` | ✅ `CornerCases.coordinatorCrashesDuringWriteOkDissemination` + Demo 4 |
| 3 | **Due nodi consecutivi** crashano durante l'elezione | `Crash(Now,0)` su id adiacenti | ✅ `CornerCases.twoConsecutiveReplicasCrashDuringElection` + `RingTopologyTest` |
| 4 | Vincitore crasha **prima della Synchronization** | `Crash(Election, 1)` | ✅ `CornerCases.electionWinnerCrashesBeforeSynchronization` |
| 5 | Replica crasha **dopo l'ACK**, prima di applicare | `Crash(WriteOK, 0)` | ✅ `CornerCases.replicaCrashesAfterAckBeforeApplying` |
| 6 | Client contatta una replica crashata | `Crash(Now, 0)` | ✅ `APICompliance.crashReplicaAndTryRequests` |

---

## 19. Demo eseguibili

`Main.java` contiene i quattro scenari raccomandati dalla traccia §4
(*"three or four representative execution examples, including corner cases"*).
Ogni demo costruisce il proprio `ActorSystem`, lo guida dall'esterno con
richieste del client e comandi di crash, e lo termina prima che parta la
successiva: il log di uno scenario si legge da solo. `N = 5`, coordinatore
iniziale `0`.

```bash
./gradlew run                 # tutti e quattro in sequenza (~35 s)
./gradlew run --args="3"      # solo lo scenario 3
```

### Demo 1 — happy path (`Main.java:78`)
Tre write e una read da un client attaccato alla Replica 4. Si osservano gli id
consecutivi `<0,1> <0,2> <0,3>`, i cinque ACK, il `quorum reached`, e le cinque
repliche che applicano nello stesso ordine. La read finale legge `30`, cioè
l'ultima write dell'ordine totale.

### Demo 2 — crash di una replica non coordinatrice (`:105`)
Dopo una prima write, la Replica 3 riceve `Crash(Now, 0)`. La write successiva
raggiunge comunque il quorum (4 repliche vive su 5, ne bastano 3) e viene
applicata dalle sole repliche vive. Il secondo client, che continua a parlare
con la replica morta, va in `TIMEOUT READ`.

### Demo 3 — crash del coordinatore, elezione e sincronizzazione (`:136`)
Il coordinatore 0 muore fra due write. La write emessa subito dopo resta
bloccata sulla Replica 2, che l'aveva inoltrata. Dopo ~3 s scattano gli
`HEARTBEAT TIMEOUT`, parte l'elezione ad anello, tutte le repliche hanno lo
stesso `latestId` e il tie-break assegna la vittoria alla Replica 4; la
`SYNCHRONIZATION` apre l'epoch 1 e la Replica 2 rigioca la write bufferizzata,
applicata come `<1,1>`. Il client riceve il suo `WRITE complete` diversi
secondi dopo averla emessa, **senza aver ritentato nulla**.

### Demo 4 — WRITEOK parzialmente disseminato (`:168`)
Il coordinatore è armato con `Crash(WriteOK, 2)`: raggiunge il quorum, invia il
WRITEOK a due repliche (sé stesso — che però è già morto quando gli tornerebbe
indietro — e la Replica 1) e muore a metà broadcast, lasciando il log
`crashed while broadcasting WriteOk(<0,1>)`. Da quel momento **una sola replica
al mondo ha applicato `<0,1>`**. L'elezione la premia proprio per questo
(`ring lap complete, winner is 1`), e la sua `SYNCHRONIZATION` porta l'update
alle altre tre. Il client, attaccato alla Replica 4, riceve
`WRITE complete (true, 0, 99, 4)` e la read finale legge `99` da una replica
che quel valore l'ha conosciuto solo attraverso la sincronizzazione.

È lo scenario che dimostra dal vivo la property della traccia: *"if a replica a
applies an update w, then all correct replicas will eventually apply w"*.
Traccia estratta da un'esecuzione reale:

```
[Replica 0] quorum reached for <0,1> -> WRITEOK
[Replica 0] crashed while broadcasting WriteOk(<0,1>): the remaining replicas will not receive it
[Replica 1] applied update 0:1 (0, 99)
[Replica 2] HEARTBEAT TIMEOUT: Coordinator 0 is suspected to have crashed
[Replica 4] ring lap complete, winner is 1: forwarding
[Replica 1] WON the election, completing interrupted updates before epoch 1
[Replica 1] NEW COORDINATOR elected: 1
[Replica 2] SYNCHRONIZATION from 1: epoch 1, 1 update(s) to replay
[Client client] WRITE complete (true, 0, 99, 4)
[Client client] READ complete (true, 0, 99, 4)
```

---

# Parte V — Stato e residui

## 20. Cosa manca da fare

In ordine di priorità.

### 20.1 Report LaTeX (Sprint 5.1) — **unico blocco alla consegna**

`report/main.tex` compila ma le tre sezioni sono **file di una riga con il solo
`\section{}`**: `01_structure.tex`, `02_design.tex`, `03_implementation.tex`.
Anche `\author{Surname1 Name1, Surname2 Name2}` è ancora il placeholder del
template.

Serve scrivere 3-4 pagine (max 6, oltre le quali il progetto viene rifiutato
d'ufficio) in inglese, rispondendo alle domande delle slide:

- scelte architetturali (perché la logica di elezione è pura e separata dalla FSM);
- gestione dei timeout e valori scelti (§16) con la giustificazione del "no
  false positives";
- topologia del ring e perché la sequenza di hop invece dell'aritmetica modulare;
- trattamento degli update orfani e ordine delle fasi in `becomeWinner`;
- motivazione del tie-break e della sentinella `NONE`;
- perché `Synchronization` è un broadcast unico calcolato sul watermark;
- come è istrumentato il crash a metà broadcast (§9.7) e cosa dimostra la Demo 4;
- **tutte** le assunzioni aggiuntive di §22 (la traccia lo richiede
  esplicitamente).

Va incluso anche il disclaimer sull'uso di assistenza AI.

### 20.2 Consegna (Sprint 5.3)

- [ ] `./gradlew test` verde su clone pulito (il wrapper è ora committato,
      §21.1: basta clonare ed eseguire);
- [ ] report `.pdf` autocontenuto;
- [ ] archivio `tar -czvf CognomeACognomeB.tgz CognomeACognomeB/` con sorgenti
      + report, **senza** i PDF del prof in `docs/`;
- [ ] prenotazione dello slot via mail a Picco + Pasquali + Genetti prima della
      deadline dello slot;
- [ ] indicare in-person vs online;
- [ ] preparare i 12 minuti (timer rigido) + Q&A, usando le quattro demo e i
      cinque corner case come materiale.

---

## 21. Osservazioni emerse dall'audit del 2026-08-22

L'audit ha trovato quattro imperfezioni — nessuna faceva fallire un test, due
avevano impatto sulla consegna. **Sono state tutte risolte il 2026-08-22**; le
sezioni che seguono conservano il problema e il rimedio, perché è materiale
utile per il report e per l'orale. La suite è stata rieseguita dopo le
correzioni: **98/98 verdi**.

### 21.1 Il wrapper Gradle non era tracciato da git — ✅ risolto

`.gitignore` conteneva `gradlew`, `gradlew.bat` e `**/gradle`, quindi il
wrapper **non era committato** (verificato con `git ls-files`). Sul working
tree i file c'erano, ma:

- il criterio di uscita "`./gradlew build` verde da clone pulito" **non era
  soddisfatto**: chi clonava il repo doveva prima rigenerare il wrapper con
  `gradle wrapper --gradle-version 9.2.1`;
- se l'archivio di consegna fosse stato prodotto da un clone invece che dal
  working tree, i correttori non avrebbero potuto compilare — e la traccia §2
  è netta: *"Submissions that do not pass ALL tests will not be considered"*.

**Rimedio applicato**: rimosse le tre righe da `.gitignore` (lasciando ignorate
solo le cache `.gradle/` e gli output `build/`), aggiunta l'eccezione esplicita
`!gradle/wrapper/gradle-wrapper.jar` per neutralizzare la regola `*.jar`, e
committati `gradlew` (con il bit di esecuzione, modo `100755`), `gradlew.bat`,
`gradle/wrapper/gradle-wrapper.jar` e `gradle-wrapper.properties`. È anche la
pratica raccomandata da Gradle.

### 21.2 Refusi nei commenti — ✅ risolti

Non toccavano il comportamento, ma la traccia §4 valuta anche la forma
(*"submissions that do not follow reasonable coding standards will not be
accepted"*). Nove correzioni in `Replica.java` (`awating` → `awaiting`,
`callbackOnElectionSTarted` → `callbackOnElectionStarted`, `left aliv` →
`left alive`, `we havea lready won` → `we have already won`, `Puleld into` →
`Pulled into`) e in `CornerCases.java` (`succeded` → `succeeded` ×2, `bufered`
→ `buffered`, `surviced` → `survived`, `reason tio` → `reason to`).

### 21.3 Doppio conteggio del crash counter sul coordinatore — ✅ risolto

Il commento di `checkCrashCondition` affermava che *"per un dato tipo una
replica o è quella che lo broadcasta o è una delle destinatarie, mai
entrambe"*. Non era vero: `broadcast` include **anche il coordinatore stesso**
fra i destinatari, quindi per `Update`, `WriteOK` e `Heartbeat` il coordinatore
incrementava il contatore due volte per lo stesso messaggio — una in invio, una
alla consegna a sé stesso — falsando la semantica
`after_n_messages_of_type` fissata dalla codebase (regola 10).

Effetto pratico: nullo per i valori usati nei test e nelle demo (con
`Crash(WriteOK, 2)` e N=5 la replica muore durante il broadcast, molto prima di
ricevere alcunché); visibile con `after_n ≥ N`.

**Rimedio applicato**: gli handler in ricezione di `UpdateMsg`, `WriteOk` e
`Heartbeat` passano ora da `checkIncomingCrashCondition` (`Replica.java:911`),
che salta il conteggio quando siamo noi ad aver fatto quel broadcast:

```java
private boolean checkIncomingCrashCondition(AbstractReplica.Crash.Type type) {
    return isCoordinator() || checkCrashCondition(type);
}
```

Regola risultante, ora esatta: **per un tipo che viaggia in broadcast, chi lo
manda conta in uscita e tutti gli altri contano in entrata; ogni messaggio
avanza il contatore esattamente una volta.** `Election` non passa mai da
`broadcast` e il suo handler continua a chiamare direttamente
`checkCrashCondition`. La tabella di §9.7 resta valida alla lettera; il
javadoc è stato riscritto di conseguenza.

### 21.4 Una `Election` ritardataria poteva far rientrare in elezione un coordinatore sano — ✅ risolto

`isStaleElection` riconosceva come "stantia" un'`Election` il cui payload
eleggeva il coordinatore che si stava già seguendo. Se però il payload che
arrivava al nuovo coordinatore **non conteneva ancora la sua entry** (per
esempio il primo hop di un round riavviato dal `GlobalElectionTimeout` di una
replica rimasta indietro), il vincitore calcolato era un altro e la guardia non
scattava: il coordinatore in carica entrava in ELECTION e — avendo la history
più aggiornata — vinceva di nuovo, bruciando un epoch. Non violava la safety,
ma era un'elezione superflua.

**Rimedio applicato**, in tre punti e aderente al modello di guasto della
traccia (*"Crash detection is assumed to be accurate and does not produce false
positives"*):

1. `isStaleElection` (`:598`) considera stantia **ogni** `Election` che
   raggiunge il coordinatore in carica: se siamo vivi e siamo il coordinatore,
   nessuna replica corretta può legittimamente sospettarci, quindi quel round
   riguarda un crash precedente già risolto. La replica **non** entra più in
   ELECTION e risponde invece con una `Synchronization`, che è ciò di cui il
   mittente ha bisogno per uscire dal round;
2. il payload di quella `Synchronization` è calcolato da `syncPayloadFor`
   (`:616`), che tollera un'`Election` senza entry restituendo l'intera history
   invece di far esplodere `SyncPlan.missingForAll` con
   `IllegalArgumentException` (caso degenere che prima era evitato solo da una
   guardia sull'insieme vuoto);
3. `onSynchronization` (`:739`) esegue `suspected.remove(msg.newCoordinatorId)`:
   una replica trascinata in un round su un coordinatore già sostituito
   sospetta proprio quest'ultimo, e il messaggio è **prova che è vivo**, quindi
   non deve restare nella blacklist del ring.

La regola generale del protocollo — *"When a replica receives an ELECTION
message, if it is not already participating in the election, it adds its own
information"* — resta invariata per tutte le repliche che **non** sono il
coordinatore: una replica che non ha ancora rilevato il crash continua a essere
trascinata nel round dal vicino, come richiede la traccia.

### 21.5 API definite ma non usate

Non è un difetto — sono API di comodo del modello dati — ma va saputo:

- `UpdateID.nextEpoch()` — il nuovo epoch si calcola su tutto il payload
  dell'elezione (`ElectionLogic.newEpoch`), non dall'id del vincitore;
- `UpdateHistory.size()`, `isEmpty()` e `latest()` (quest'ultimo usato solo
  internamente da `latestId()`). `asList()` è invece entrato in uso con la
  correzione §21.4, dentro `Replica.syncPayloadFor`;
- `SyncPlan.missingFor(...)` è chiamato dal protocollo solo indirettamente,
  attraverso `missingForAll`; direttamente solo dai test;
- `Election.initiatorId` viaggia e viene loggato ma non decide nulla;
- `Logger.setDestinationFile(...)`, `setLoggingEnabled(...)`, `enable()` non
  sono usati dal nostro codice (`Logger` è codebase).

### 21.6 Il contratto di Fase 0 è stato chiuso e assorbito — ✅ risolto

Il file `CONTRACT_PHASE0.md` riportava ancora `→ Scelta: ______` per le
decisioni D1-D5 benché tutte e cinque fossero di fatto chiuse dal codice. È
stato **assorbito in `ROADMAP.md` → Fase 0** (§F0.1-F0.9), con l'esito di
ciascuna decisione annotato, ed eliminato come file a sé stante.

Da ricordare per l'orale: **D4 è stata risolta al contrario della
raccomandazione iniziale**. Il contratto suggeriva di *droppare* le write che
arrivano durante un'elezione, contando su un ritento del client; il `Client` di
questo progetto però emette ogni richiesta una sola volta e non ritenta, quindi
droppare significherebbe perderla. Le write vengono perciò **bufferizzate**
(`onClientWriteDuringElection`) e rigiocate al nuovo coordinatore
(`replayPendingClientWrites`).

---

## 22. Limitazioni note e assunzioni da dichiarare nel report

Sono scelte difendibili, ma vanno **dichiarate** (traccia §2: *"It is important
to state all additional assumptions in the report"*).

1. **I messaggi già accodati sopravvivono al crash.** I `NetworkChannel` sono
   figli della replica e non vengono fermati da `triggerCrash()`, quindi ciò
   che il mittente aveva già consegnato al canale arriva comunque. Il crash
   impedisce di *iniziare* nuovi invii, non di completare quelli in volo — ed è
   proprio questo che rende il broadcast parziale di §9.7 un troncamento del
   ciclo di invio e non una cancellazione di messaggi già partiti.
2. **`ElectionAck` è vuoto (decisione D1)**: `onElectionAck` (`:623`) cancella
   il timer corrente senza verificare da chi arriva. Un ack in ritardo di un
   successore già saltato può quindi cancellare un timer appena riarmato per un
   altro successore; l'effetto peggiore è un round più lento, coperto dal
   `GlobalElectionTimeout`. Correlare l'ack con l'id del mittente sarebbe una
   modifica di due righe.
3. **Una `Synchronization` molto in ritardo può interrompere un'elezione.** La
   guardia sui duplicati non scatta quando la replica sta partecipando a un
   round: se l'annuncio di un coordinatore ormai morto arriva in quel momento,
   la replica esce dall'elezione e lo adotta, per poi riaccorgersi della sua
   morte al successivo `HeartbeatTimeout`. Il sistema converge lo stesso, con
   un'elezione in più. In quel caso il `suspected.remove` di §21.4 toglierebbe
   dalla blacklist una replica in realtà morta: costerebbe un
   `ElectionAckTimeout` in più nel giro successivo, dopo il quale verrebbe
   ri-sospettata. La finestra è larga quanto un hop.
4. **Una replica viva ma lenta può essere sospettata** se non ack-a entro
   `getMaxLatencyPlusTolerance()`. Verrebbe esclusa dal payload dell'`Election`
   e quindi dal calcolo del watermark di `missingForAll`, restando
   potenzialmente indietro. La traccia però assume esplicitamente che la crash
   detection sia accurata, quindi lo scenario è fuori dal modello.
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
8. **Il traffico client → replica non passa dal `NetworkChannel`.**
   `AbstractClient` non espone un helper `tell(...)` con canale, quindi
   `Client` usa `ActorRef.tell` diretto: quella direzione non ha ritardo
   simulato, mentre replica → client e replica → replica ce l'hanno. È il
   design della codebase fornita, ma è un'assunzione da dichiarare.
9. **Le write che arrivano durante un'elezione sono bufferizzate**, non
   droppate (decisione D4, risolta diversamente da come il contratto iniziale
   raccomandava). Il buffer non è persistente né limitato: se una replica
   accumulasse un numero enorme di write durante un'elezione lunga, crescerebbe
   senza freni. Nel modello della traccia (elezioni brevi, client di test) non è
   un problema.

---

## 23. Come compilare ed eseguire

Serve **solo un JDK 17+** (il progetto è compilato con Java 21): il wrapper è
committato nel repository e scarica da sé Gradle 9.2.1, quindi da un clone
pulito basta lanciare `./gradlew`.

```bash
./gradlew build                                  # compila tutto ed esegue i test (~2 min)
./gradlew test                                   # solo i test
./gradlew test --tests "*NoCrashes*"             # consistenza sequenziale
./gradlew test --tests "*WithCrashes*"           # crash + elezione
./gradlew test --tests "*APICompliance*"         # conformità alle API
./gradlew test --tests "*CornerCases*"           # corner case del fault model
./gradlew test --tests "it.unitn.ds.election.*"  # unit test puri (millisecondi)
./gradlew test --rerun-tasks                     # forza la riesecuzione (utile contro la flakiness)
./gradlew clean

./gradlew run                                    # i quattro scenari di demo (~35 s)
./gradlew run --args="4"                         # solo lo scenario 4
```

Report HTML dei test: `build/reports/tests/test/index.html`.
Durante i test i log sono disabilitati (`TestsCommons.DO_PRINTS = false`)
perché, come avverte la traccia, le stampe possono interferire con l'esito;
nelle demo invece sono attivi su stdout (`Logger.setDestinationStdout()`).
Per abilitare anche i `debug(...)`: `Logger.setDebugEnabled(true)` in `Main`.
