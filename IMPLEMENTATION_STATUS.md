# Stato implementativo

Documento di stato del progetto **Quorum-Based Total Order Broadcast**
(Distributed Systems 2025-2026). Spiega cosa è stato implementato finora,
come è stato strutturato e perché. Identificatori, log e nomi di file
restano in inglese; il commento in prosa è in italiano come da convenzione
del corso.

Per la roadmap completa con criteri di uscita per sprint vedere
[`ROADMAP.md`](ROADMAP.md).

---

## Indice

1. [Layout del repository](#layout-del-repository)
2. [Sprint 0 — Setup](#sprint-0--setup-completato)
3. [Sprint 1.1 — Modello dati](#sprint-11--modello-dati-completato)
4. [Sprint 1.2 — Messaggi di protocollo](#sprint-12--messaggi-di-protocollo-completato)
5. [Sprint 1.3 — Client e Replica (happy path)](#sprint-13--client-e-replica-happy-path-completato)
6. [Cosa non è ancora implementato](#cosa-non-è-ancora-implementato)
7. [Come compilare ed eseguire i test](#come-compilare-ed-eseguire-i-test)

---

## Layout del repository

```
SD_PROJECT/
├── build.gradle                # configurazione Gradle (Akka 2.6 + JUnit 5)
├── gradlew, gradlew.bat        # wrapper Gradle 9.2.1
├── gradle/                     # distribuzione wrapper
├── README.md                   # introduzione al progetto
├── ROADMAP.md                  # piano per sprint (source-of-truth operativo)
├── IMPLEMENTATION_STATUS.md    # questo documento
├── docs/
│   ├── ds1_project_2026.pdf                # traccia ufficiale
│   ├── ds1_project_2026_presentation.pdf   # slide di progetto
│   └── PROGETTO SD.pdf                     # guida di pianificazione interna
├── report/                     # skeleton report LaTeX (Sprint 5)
└── src/
    ├── main/java/it/unitn/ds/
    │   ├── AbstractClient.java          # codebase obbligatoria (Genetti/Pasquali)
    │   ├── AbstractReplica.java         # codebase obbligatoria
    │   ├── Client.java                  # Sprint 1.3 — read/write + timeout
    │   ├── Replica.java                 # Sprint 1.3 — happy path two-phase
    │   ├── Logger.java                  # logging timestamped (codebase)
    │   ├── Main.java                    # entry point demo
    │   ├── NetworkChannel.java          # canale FIFO con latenza random
    │   ├── UpdateID.java                # Sprint 1.1
    │   ├── Update.java                  # Sprint 1.1
    │   ├── UpdateHistory.java           # Sprint 1.1
    │   └── messages/                    # Sprint 1.2
    │       ├── ClientRead.java        # Sprint 1.3 (client -> replica)
    │       ├── ClientWrite.java       # Sprint 1.3 (client -> replica)
    │       ├── ReadReply.java         # Sprint 1.3 (replica -> client)
    │       ├── WriteReply.java        # Sprint 1.3 (replica -> client)
    │       ├── ForwardWrite.java
    │       ├── UpdateMsg.java
    │       ├── UpdateAck.java
    │       ├── WriteOk.java
    │       ├── Heartbeat.java
    │       ├── Election.java
    │       ├── ElectionAck.java
    │       ├── Synchronization.java
    │       ├── UpdateTimeout.java
    │       ├── ForwardTimeout.java
    │       ├── HeartbeatTimeout.java
    │       ├── ElectionAckTimeout.java
    │       └── GlobalElectionTimeout.java
    └── test/java/it/unitn/ds/
        ├── TestsCommons.java
        └── base/
            ├── NoCrashes.java           # test happy path
            ├── WithCrashes.java         # test con crash istrumentati
            └── APICompliance.java       # test contrattuali codebase
```

## Sprint 0 — Setup (COMPLETATO)

- Riorganizzazione delle cartelle in `docs/`, `report/`,
  `src/main/java/it/unitn/ds/`, `src/test/java/it/unitn/ds/base/`.
- Generato il **Gradle wrapper 9.2.1** (`./gradlew`) per build riproducibili.
- `build.gradle` configurato con Akka classic 2.6 + JUnit 5.
- README di progetto.
- Repository git inizializzato e pushato su
  `git@github.com:Teto03/Distributed_Denial_of_Systems.git`.

**Verifica**: `./gradlew build` su clone pulito → BUILD SUCCESSFUL.

---

## Sprint 1.1 — Modello dati (COMPLETATO)

Tre tipi base, tutti `Serializable`, in `src/main/java/it/unitn/ds/`.

### `UpdateID`

Coppia logica `<epoch, sequence>` che identifica univocamente un update nel
protocollo two-phase. È **immutabile**, `Serializable`,
`Comparable<UpdateID>` con ordinamento lessicografico (prima `epoch`, poi
`sequence`).

Helper di costruzione:

- `nextInEpoch()` → `<epoch, sequence+1>`, usato dal coordinatore quando
  assegna una nuova UpdateID nello stesso epoch.
- `nextEpoch()` → `<epoch+1, 0>`, usato dal nuovo coordinatore appena
  eletto dopo aver completato la fase di sincronizzazione (Sprint 3).

Equals/hashCode su entrambi i campi; `toString()` produce `<e,i>` per i log
formattati.

Riferimento traccia §1 "Update protocol": *"Each update is uniquely
identified by a pair ⟨e, i⟩"*.

### `Update`

Record immutabile `(UpdateID id, int index, int value)` che rappresenta
**una singola entry deliverata** nella history della replica. Nessun
riferimento a `ActorRef`: questa è la rappresentazione persistente del log
locale, indipendente dalla replica che l'ha applicata.

Equals/hashCode su `(id, index, value)`; `toString()` rispetta il pattern di
log: `<e,i> positions[index]=value`.

### `UpdateHistory`

Log **append-only** di `Update` per replica. Logicamente immutabile (solo
`append()`, mai rimozioni o riordini); istanza mutabile a fini di
bookkeeping locale. Le API per inviare snapshot via rete passano dalla
copia difensiva di `asList()`.

Metodi:

- `append(Update)` — aggiunge una entry.
- `size()`, `isEmpty()` — utility.
- `latest()` → `Optional<Update>` — ultima entry; usata sia per la scelta
  del nuovo coordinatore (chi ha il `latestId()` più alto vince) sia come
  punto di partenza per la sincronizzazione.
- `latestId()` → `Optional<UpdateID>` — comodo wrapper.
- `asList()` — snapshot immutabile da serializzare in
  `Synchronization`.
- `after(UpdateID threshold)` — sottolista strettamente più recente di
  `threshold`, ordinata. Servirà nel calcolo dei pending update da
  replayare in Sync.

Riferimento traccia §1 "Coordinator election": *"the replica that knows
the most recent update"*.

---

## Sprint 1.2 — Messaggi di protocollo (COMPLETATO)

Sotto-package `it.unitn.ds.messages/` con 13 classi: tutte `final`,
`Serializable`, campi `public final`, senza setter. La scelta del sub-
package è quella suggerita dalla guida interna (`PROGETTO SD.pdf`, §3).

> **Nota di nomenclatura.** Il ROADMAP iniziale chiamava il broadcast del
> coordinatore semplicemente `Update`, ma quel nome è già usato dal data
> class della history (vedi 1.1). Per evitare la collisione il messaggio è
> stato rinominato in **`UpdateMsg`**, e l'`Update` data class continua a
> rappresentare l'entry persistita. `UpdateMsg` *wrappa* un `Update` (più
> i campi di routing client/replica), così la stessa istanza
> immutabile può essere appesa alla `UpdateHistory` senza
> riallocazioni.

### Protocollo two-phase (happy path)

| Classe          | Direzione                   | Campi                                                                         | Note |
|-----------------|-----------------------------|-------------------------------------------------------------------------------|------|
| `ForwardWrite`  | Replica → Coordinatore      | `int index`, `int value`, `ActorRef client`, `int contactedReplicaId`         | Trasporta `contactedReplicaId` perché il `WriteResult` finale deve avere `fromReplica == replica contattata dal client` (regola 11 della codebase). |
| `UpdateMsg`     | Coordinatore → Repliche     | `Update update`, `ActorRef client`, `int contactedReplicaId`                  | Phase-1 broadcast. Wrappa un `Update` immutabile + routing info. |
| `UpdateAck`     | Replica → Coordinatore      | `UpdateID id`                                                                 | Risposta phase-1. Il coordinatore aggrega per `id` fino a `⌊N/2⌋+1` ack. |
| `WriteOk`       | Coordinatore → Repliche     | `UpdateID id`                                                                 | Phase-2 broadcast. Ogni replica applica `positions[idx]=val`, appende a `UpdateHistory`, chiama `callbackOnUpdateApplied`. |

### Liveness / heartbeat

| Classe       | Direzione                | Campi  | Note |
|--------------|--------------------------|--------|------|
| `Heartbeat`  | Coordinatore → Repliche  | nessuno| Definito ora; emissione periodica e `HeartbeatTimeout` arrivano in Sprint 2. |

### Elezione (definita ora, handler in Sprint 3)

| Classe            | Direzione                 | Campi                                                          | Note |
|-------------------|---------------------------|----------------------------------------------------------------|------|
| `Election`        | Replica → Replica (ring)  | `int initiatorId`, `Map<Integer, UpdateID> latestPerReplica`   | Ogni replica accumula il proprio `latestId()` nel map prima di forwardare al successore. Mappa difensivamente copiata + `unmodifiableMap`. |
| `ElectionAck`     | Replica → Replica (ring)  | nessuno                                                        | Ack hop-by-hop: il sender della `Election` arma un `ElectionAckTimeout` e, se scade, salta il successore silenzioso. |
| `Synchronization` | Nuovo coord → Repliche    | `int newCoordinatorId`, `int newEpoch`, `List<Update> pendingUpdates` | Il vincitore replaya gli update in flight prima di bumpare l'epoch (uniform agreement recovery). Lista difensivamente copiata + `unmodifiableList`. |

### Timer self-messages

Tutti `Serializable` per coerenza, anche se schedulati via
`scheduler().scheduleOnce(...)` (non passano per `NetworkChannel`).

| Classe                   | Trigger                                                                                 | Note |
|--------------------------|------------------------------------------------------------------------------------------|------|
| `UpdateTimeout(id)`      | Replica che ha ack-ato ma non riceve `WriteOk(id)` in tempo.                            | Sprint 3 lo collegherà al sospetto del coordinatore. |
| `ForwardTimeout(idx,val)`| Non-coordinatore che ha inoltrato `ForwardWrite` ma non vede il proprio `WriteOk`.       | Trigger di `WriteTimeout` lato client in Sprint 1. |
| `HeartbeatTimeout`       | Non-coordinatore che non riceve heartbeat entro `k × coordinatorBeatInterval`.           | Wiring in Sprint 2, hook all'elezione in Sprint 3. |
| `ElectionAckTimeout(s)`  | Sender di `Election` che non riceve `ElectionAck` dal successore `s`.                    | Lo skip-and-forward riferisce traccia §1: *"a replica that forwards an ELECTION message starts a timeout while waiting for the corresponding ACK"*. |
| `GlobalElectionTimeout`  | Rete di sicurezza anti-livelock (Hint-2 della guida interna).                            | Riavvia un'elezione da zero se l'attuale non termina. |

### Vincoli di design rispettati

- **Immutabilità**: tutti i campi sono `public final`; le collezioni passano
  da copia difensiva + `unmodifiable*` (vedi `Election`, `Synchronization`).
  Necessario per la regola della traccia §2: *"any shared objects must be
  immutable"*.
- **Serializable**: presente su ogni classe — anche sui timeout, per
  uniformità (sebbene non strettamente richiesto per messaggi self).
- **Nessuna collisione di nomi**: il data class `Update` resta intoccato;
  il broadcast è `UpdateMsg`.
- **`toString()` parlanti**: ogni messaggio ha un `toString()` compatto che
  rispetta lo stile dei log richiesti dalla traccia §2.
- **`Objects.requireNonNull`** sui riferimenti non opzionali (UpdateID,
  ActorRef del client, Update wrappato) per fallire presto su bug.

---

## Sprint 1.3 — Client e Replica (happy path) (COMPLETATO)

Implementazione del percorso "tutto funziona": nessun crash, nessuna
elezione. Corrisponde alla traccia §1 ("Client requests" + "Update
protocol").

### Quattro nuovi messaggi client ↔ replica

Le `AbstractClient.ReadRequest`/`WriteRequest` del codebase sono i messaggi
che il *test harness* manda al client; servivano quindi messaggi propri per
il dialogo client → replica → client. Sono in `messages/`, tutti immutabili
e `Serializable`:

| Classe        | Direzione        | Campi                                          |
|---------------|------------------|------------------------------------------------|
| `ClientRead`  | client → replica | `long reqId`, `int index`                      |
| `ClientWrite` | client → replica | `long reqId`, `int index`, `int value`         |
| `ReadReply`   | replica → client | `long reqId`, `int index`, `int value`, `int fromReplica` |
| `WriteReply`  | replica → client | `long reqId`, `int index`, `int value`, `int fromReplica` |

Il `reqId` è un identificatore locale del client: serve per accoppiare la
risposta (o il timeout) alla richiesta che l'ha generata quando ci sono più
richieste in volo. Per questo è stato aggiunto anche a `ForwardWrite` e
`UpdateMsg`, così viaggia con la write fino al `WriteOk` e la replica
contattata sa a chi e con quale id rispondere.

### Replica

- `initSystem` salva `group` e `coordinatorId`.
- `getSystemNumberOfActors` ritorna `group.size()`.
- **Read**: servita localmente, risponde con `ReadReply(... value=P[idx],
  fromReplica=self)`.
- **Write su replica contattata**: se è il coordinatore avvia subito il
  broadcast, altrimenti inoltra `ForwardWrite` al coordinatore.
- **Coordinatore**: assegna `<epoch, seq+1>`, broadcasta `UpdateMsg` a tutte
  le repliche (sé stesso incluso), conta gli `UpdateAck` e al raggiungimento
  del quorum `⌊N/2⌋+1` broadcasta `WriteOk`.
- **WriteOk** (ogni replica): applica `P[idx]=val`, appende a
  `UpdateHistory`, logga `applied update <e>:<i> (idx, val)` e chiama
  `callbackOnUpdateApplied`. La sola replica contattata risponde al client
  con `WriteReply` (così `WriteResult.fromReplica` = replica contattata,
  regola 11).

### Client

- `sendRead`/`sendWrite`: generano un `reqId`, inviano `ClientRead`/
  `ClientWrite` e armano un timeout self-schedulato.
- Su `ReadReply`/`WriteReply` chiamano `callbackOnReadResult`/
  `callbackOnWriteResult`.
- Il timeout (`ReadTick`/`WriteTick`, messaggi interni inviati solo a sé)
  fa scattare `callbackOnReadTimeout`/`callbackOnWriteTimeout` **solo** se la
  risposta non è ancora arrivata: l'arrivo della reply rimuove il `reqId`
  dall'insieme dei pending, così un tick tardivo viene semplicemente
  ignorato (niente `Cancellable` da gestire).

`crash(...)` resta volutamente vuota: la modalità `CRASHED` e i contatori
per tipo sono Sprint 2.

---

## Cosa non è ancora implementato

`Main.java` è ancora uno scaffold (gli scenari di demo sono Sprint 5).

Heartbeat periodico, stato `CRASHED` via `become`, e i contatori di crash
per tipo (`Crash.Type.{Now, Heartbeat, Update, WriteOK, Election}`) sono in
**Sprint 2**.

Elezione ring, sincronizzazione, completamento degli update orfani sono in
**Sprint 3**.

Stato dei test (`./gradlew test`):

- `NoCrashes` ✅ verde (4/4 casi).
- `APICompliance` → happy path verde: `oneClientWriteWaitRead` (4/4),
  `callbackOnUpdateAppliedInvokedOnAllReplicas` (4/4),
  `callbackOnUpdateAppliedOncePerWrite` (2/2). I casi su crash
  (`replicasCrashNow`, `crashReplicaAndTryRequests`) ed elezione restano
  rossi: attesi in Sprint 2/3.
- `WithCrashes` ❌ atteso rosso (richiede crash + elezione, Sprint 2-3).

---

## Come compilare ed eseguire i test

Richiede solo JDK 17+ (no installazione di Gradle separata; il wrapper
gestisce tutto).

```bash
# Build completo (compila main + test + esegue test)
./gradlew build

# Solo compilazione main
./gradlew compileJava

# Solo compilazione test
./gradlew compileTestJava

# Esegui solo i test del happy path (utili da Sprint 1.3 in poi)
./gradlew test --tests "*NoCrashes*"

# Esegui i test contrattuali della codebase
./gradlew test --tests "*APICompliance*"

# Pulizia totale
./gradlew clean
```

Per la demo interattiva:

```bash
./gradlew run        # se configurato in build.gradle, lancia Main
# oppure
./gradlew compileJava && java -cp build/classes/java/main:$(./gradlew -q printClasspath) it.unitn.ds.Main
```

(Il `Main.java` corrente è uno scaffold senza client: produrrà solo il
banner di start/end finché Sprint 5 non aggiungerà gli scenari di demo.)
