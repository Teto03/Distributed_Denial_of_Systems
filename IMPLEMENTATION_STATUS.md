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
8. [Sprint 3 [B] — Parti statiche di elezione e sincronizzazione](#sprint-3-b--parti-statiche-di-elezione-e-sincronizzazione-completato)

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
    │   ├── election/                    # Sprint 3 [B] — logica pura di elezione
    │   │   ├── RingTopology.java        # ordine del ring + successore vivo
    │   │   ├── ElectionLogic.java       # payload Election + scelta del vincitore
    │   │   └── SyncPlan.java            # diff degli update da replayare
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
        ├── base/
        │   ├── NoCrashes.java           # test happy path
        │   ├── WithCrashes.java         # test con crash istrumentati
        │   └── APICompliance.java       # test contrattuali codebase
        └── election/                    # Sprint 3 [B] — unit test puri
            ├── RingTopologyTest.java
            ├── ElectionLogicTest.java
            └── SyncPlanTest.java
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
**Sprint 3**: la logica pura (`election/`) è già pronta — vedi
[Sprint 3 [B]](#sprint-3-b--parti-statiche-di-elezione-e-sincronizzazione-completato)
— mentre il cablaggio dentro `Replica` è la parte di integrazione `[A+B]`.

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

---

## Sprint 3 [B] — Parti statiche di elezione e sincronizzazione (COMPLETATO)

> **Branch**: `sprint3-election` (partito dal commit di `main` con la Fase 0
> verde, come previsto da `CONTRACT_PHASE0.md` §9).
>
> **Perimetro**: solo i task taggati **[B]** in `ROADMAP.md` → Sprint 3, cioè
> la logica *pura* che non richiede runtime né FSM. Niente è stato toccato
> fuori dal package `election/` e dai suoi test: **`Replica.java` non è stato
> modificato**, così il lavoro su Sprint 2 procede in parallelo senza
> conflitti di merge (ownership di `CONTRACT_PHASE0.md` §1 rispettata).

### Perché tutto in un package separato

Le quattro voci [B] della roadmap hanno una caratteristica in comune: sono
**funzioni pure** dei dati del protocollo (insieme dei membri, payload della
`Election`, `UpdateHistory`). Isolarle in `it.unitn.ds.election` come classi
`final` con solo metodi `static` e costruttore privato dà tre vantaggi:

1. sono testabili **senza `ActorSystem`**, quindi i test girano in
   millisecondi e sono deterministici (nessuna latenza random di mezzo);
2. l'integrazione `[A+B]` diventa un lavoro di *cablaggio*: dentro `Replica`
   servirà solo decidere *quando* chiamarle, non *cosa* calcolano;
3. le invarianti di safety (chi vince, cosa va replayato) restano in un punto
   solo e sono verificabili a colpo d'occhio in fase di discussione.

Nessuna classe del package tiene stato: non c'è nulla di mutabile condiviso
fra attori, come richiesto dalla traccia §2.

### `RingTopology` — topologia del ring

Traccia §1 "Coordinator election": *"The logical ring is defined by ordering
replicas according to their identifiers"*.

```java
static List<Integer>     order(Collection<Integer> memberIds);
static Optional<Integer> successor(int self, Collection<Integer> memberIds, Set<Integer> suspected);
```

- **`order`** costruisce il ring canonico passando da un `TreeSet`: ordina in
  senso crescente **e** deduplica in un colpo solo, così che chiamarla con
  `group.keySet()` o con una lista arbitraria di id produca esattamente lo
  stesso risultato. Ritorna una lista `unmodifiable` costruita su una copia,
  quindi modificare la collezione sorgente dopo la chiamata non altera il ring
  già calcolato.
- **`successor`** è il cuore della tolleranza ai guasti durante l'elezione
  (traccia §1: *"the sender assumes that the next replica in the ring has
  crashed, skips it, and forwards the message to the following replica"*).
  L'implementazione non usa aritmetica modulare su indici — sarebbe fragile
  con id non contigui — ma costruisce la **sequenza di hop**: prima tutti gli
  id maggiori di `self` in ordine crescente, poi (wrap-around) tutti quelli
  minori. Il primo che non è in `suspected` è il successore.

  Conseguenze volute di questa formulazione:
  - `self` è escluso per costruzione → **una replica non è mai successore di
    sé stessa**, e se tutti gli altri sono sospettati si ottiene
    `Optional.empty()` invece di un loop su sé stessi;
  - `self` **non deve necessariamente appartenere** a `memberIds`: la
    partenza è il suo punto di inserimento. Utile se la vista locale ha già
    rimosso qualcuno;
  - salta **N sospettati consecutivi**, non uno solo (serve al corner case 3
    dello Sprint 4: due nodi consecutivi che crashano durante l'elezione);
  - lo skip funziona anche **attraverso il wrap-around**.

  `suspected` è un semplice `Set<Integer>` passato dal chiamante: il ring
  resta senza stato e chi possiede il set (la `Replica`, decisione D5) può
  farlo crescere come preferisce.

### `ElectionLogic` — payload e scelta del vincitore

```java
static final UpdateID NONE = new UpdateID(0, 0);
static int                    winner(Map<Integer, UpdateID> latestPerReplica);
static UpdateID               latestOf(UpdateHistory history);
static Map<Integer, UpdateID> withEntry(Map<Integer, UpdateID> latestPerReplica, int replicaId, UpdateID latest);
static int                    newEpoch(Map<Integer, UpdateID> latestPerReplica);
```

- **`winner`** implementa alla lettera la regola della traccia: *"the replica
  that knows the most recent update; replica identifiers are used to break
  ties"*. Massimo secondo l'ordine naturale di `UpdateID` (lessicografico su
  `<epoch, sequence>`, quindi un epoch più alto batte qualunque sequence), e a
  parità di `UpdateID` vince l'**id più alto**. È una funzione pura del solo
  payload: ogni replica che vede il giro completo del ring calcola lo stesso
  vincitore senza round aggiuntivi. Payload vuoto → `IllegalArgumentException`
  (situazione impossibile: chi avvia l'elezione ci mette almeno sé stesso).
- **`latestOf`** è il ponte con la history: `latestId()` oppure `NONE` se la
  replica non ha ancora deliverato niente. Rende impossibile mettere un
  `null` nel payload.
- **`withEntry`** produce il payload da forwardare: copia + entry propria, con
  `putIfAbsent`. La scelta *"first writer wins"* traduce il *"if it is not
  already participating in the election, it adds its own information"* della
  traccia: se rivedo il mio id, il messaggio ha completato il giro e il
  payload deve restare **identico** a quello visto da tutti gli altri,
  altrimenti repliche diverse potrebbero decidere vincitori diversi. Ritorna
  una mappa `unmodifiable` e **non muta** l'input (il messaggio `Election` che
  ho ricevuto resta immutabile, traccia §2).
- **`newEpoch`** calcola `max(epoch visti) + 1`. Il massimo è preso su **tutto
  il payload**, non solo sulla history del vincitore: se una replica ha visto
  un epoch più alto (perché era rimasta indietro un'elezione ma aveva ricevuto
  un update di un epoch successivo), riusare un epoch già speso romperebbe
  l'unicità degli `UpdateID`. Con history tutte vuote dà `1`, coerente col
  fatto che il coordinatore iniziale lavora in epoch `0`.

### `SyncPlan` — diff della sincronizzazione

```java
static List<Update> missingFor(UpdateHistory winnerHistory, UpdateID recipientLatest);
static List<Update> missingForAll(UpdateHistory winnerHistory, Map<Integer, UpdateID> latestPerReplica);
static UpdateID     oldest(Map<Integer, UpdateID> latestPerReplica);
```

È la metà "safety-critical" dello sprint: serve a garantire la property della
traccia §1 (*"if a replica a applies an update w, then all correct replicas
will eventually apply w"*).

- **`missingFor`** è il seam previsto dal contratto sopra
  `UpdateHistory.after(...)`: il livello di elezione non tocca mai gli
  interni della history. Rimane totale anche nei casi degeneri (destinatario
  più avanti del vincitore → lista vuota, mai un risultato "negativo").
- **`missingForAll` + `oldest`** sono un'aggiunta rispetto agli stub di Fase 0
  (§6.2 del contratto), **additiva** e quindi non rompe nulla per A. Motivo:
  `Synchronization` come congelata in §2 del contratto è un **broadcast unico**
  con una sola `List<Update> pendingUpdates`, quindi il vincitore non può
  spedire una lista personalizzata per destinatario. La lista da mettere nel
  broadcast è allora il diff calcolato rispetto alla **replica più indietro**,
  cioè il minimo dei `latestId` presenti nel payload della `Election`
  (`oldest`). Chi ha già applicato una parte di quegli update semplicemente li
  scarta: la consegna è idempotente sull'`UpdateID` (property "Integrity": una
  sola delivery per messaggio) — **nota per l'integrazione [A+B]: il
  destinatario della `Synchronization` deve filtrare gli `Update` con
  `id <= proprio latestId` prima di applicarli**, altrimenti
  `callbackOnUpdateApplied` verrebbe chiamata due volte per la stessa write.

### Decisioni di Fase 0 seguite

Nel codice ho seguito le raccomandazioni già scritte in `CONTRACT_PHASE0.md`
§7; segnalo quali toccano il mio pezzo, da confermare insieme prima del merge:

| Decisione | Scelta adottata nel codice [B] | Note |
|-----------|--------------------------------|------|
| **D1** — correlazione `ElectionAck` | ack vuoto, come da raccomandazione | non impatta il package `election/` (nessuna delle tre classi lo tocca); si rivaluta in `[A+B]` se emergono ack stantii |
| **D2** — sentinella history vuota | `ElectionLogic.NONE = <0,0>` | congelata; c'è un test che verifica `NONE < <0,1>`, cioè che sia più piccola di qualunque id reale |
| **D3** — calcolo di `newEpoch` | `max(epoch visti) + 1` via `ElectionLogic.newEpoch` | vedi sopra sul perché il massimo è su tutto il payload |
| **D4** — write durante `ELECTION` | non impatta [B] | resta ad A / integrazione |
| **D5** — sorgente del set `suspected` | `RingTopology` lo riceve come parametro, non lo possiede | la `Replica` (A) resta l'unica proprietaria del set |

### Test

Package `src/test/java/it/unitn/ds/election/`, JUnit 5, nessun `ActorSystem`.
**60 test, tutti verdi** (22 + 22 + 16).

```bash
./gradlew test --tests "it.unitn.ds.election.*"
```

Cosa coprono, oltre al caso nominale:

- `RingTopologyTest` (22) — ordinamento e deduplicazione, uso diretto di
  `group.keySet()`, immutabilità dello snapshot, successore semplice,
  wrap-around, id non contigui, **giro completo del ring** che visita ogni
  replica esattamente una volta, ring da un solo membro e ring vuoto, skip di
  uno / di due sospettati consecutivi / attraverso il wrap-around, tutti
  sospettati → `Optional.empty()`, `self` sospettato o fuori dal ring,
  argomenti `null`.
- `ElectionLogicTest` (22) — vincitore singolo, replica più aggiornata,
  epoch che batte la sequence, tie-break sull'id più alto, tie-break che si
  applica **solo** fra le repliche più aggiornate (un id alto ma indietro non
  vince), tutte le history vuote → vince l'id più alto, indipendenza
  dall'ordine di iterazione della mappa, payload vuoto e `null`;
  `latestOf` su history vuota e piena; `withEntry` che aggiunge, che preserva
  l'entry già presente, che non muta la sorgente e che ritorna
  `unmodifiable`; `newEpoch` nei casi base e nel caso in cui un partecipante
  ha visto un epoch più alto del vincitore.
- `SyncPlanTest` (16) — catch-up completo da history vuota, catch-up
  parziale, replica allineata, replica più avanti del vincitore, ordine
  totale preservato nel diff, diff che attraversa il **confine di epoch**
  (update orfano dell'epoch precedente), history del vincitore vuota,
  immutabilità del risultato, `oldest` come minimo del payload (con confronto
  epoch-prima-di-sequence), broadcast che copre la replica più indietro,
  broadcast vuoto quando sono tutti allineati, payload vuoto e `null`.

Nessuna regressione: `./gradlew test --tests "*NoCrashes*"` resta verde (4/4);
`WithCrashes` e i casi `APICompliance` su crash/elezione restano rossi come
atteso finché Sprint 2 e l'integrazione `[A+B]` non sono chiusi.

### Cosa manca dello Sprint 3 (tutto `[A+B]`, da fare in pair)

Il package `election/` è completo per la parte statica; resta il cablaggio
dentro `Replica.java`, che per contratto è di A fino al merge:

1. behavior `election()` separato (`become`) con i soli `Election`,
   `ElectionAck`, `ElectionAckTimeout`, `GlobalElectionTimeout`,
   `Synchronization`, `Crash`;
2. trigger dell'elezione dall'`HeartbeatTimeout` / `UpdateTimeout` /
   `ForwardTimeout` dello Sprint 2 (oggi logging-only);
3. `ElectionAckTimeout` → aggiunta del successore silenzioso a `suspected` e
   nuova chiamata a `RingTopology.successor(...)`;
4. broadcast della `Synchronization` con `SyncPlan.missingForAll(...)` e
   `ElectionLogic.newEpoch(...)`, **dopo** aver completato gli update
   pendenti e prima di riprendere le write;
5. filtro di idempotenza sugli `Update` replayati (vedi nota in `SyncPlan`);
6. `GlobalElectionTimeout` anti-livelock;
7. firing di `callbackOnElectionStarted` / `callbackOnCoordinatorElected` con
   il timing congelato in `CONTRACT_PHASE0.md` §8.
