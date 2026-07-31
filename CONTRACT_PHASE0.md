# Fase 0 — Contratto delle interfacce (Sprint 2–3)

> **Scopo.** Questo documento è la **base comune** che A e B congelano *prima*
> di scrivere codice di logica. Fissa i confini fra i due flussi di lavoro
> (vedi `ROADMAP.md` → "Organizzazione del lavoro a due") così che:
> 1. A (Detection & FSM, Sprint 2) e B (Election & Sync statico, Sprint 3)
>    lavorino su **file diversi** senza conflitti di merge;
> 2. il pezzo di integrazione `[A+B]` abbia firme già decise, non da negoziare
>    a metà sprint.
>
> Regola d'oro: **finché questo contratto non è verde (`./gradlew build`), non
> si aprono i due branch di lavoro.** Ogni modifica al contratto dopo l'avvio
> va fatta insieme e comunicata all'altro.

Tutto ciò che segue è **già ancorato al codice attuale** (`src/main/...`). I
messaggi e l'enum `Crash.Type` esistono già come stub dallo Sprint 1: qui li
dichiariamo *congelati*, non li reinventiamo.

---

## 1. Mappa di ownership dei file

Il vincolo anti-conflitto è questo: **A e B non toccano mai lo stesso file**
durante il lavoro parallelo. L'unico file condiviso (`Replica.java`) è di A
fino al merge; B ci mette le mani solo in pair durante l'integrazione `[A+B]`.

| File / package                                   | Owner   | Fase                     |
|--------------------------------------------------|---------|--------------------------|
| `Replica.java`                                   | **A**   | Sprint 2 + merge `[A+B]` |
| `messages/Heartbeat`, `HeartbeatTimeout`         | **A**   | Sprint 2 (già stub)      |
| `messages/UpdateTimeout`, `ForwardTimeout`       | **A**   | Sprint 2 (già stub)      |
| **`election/` (nuovo package)**                  | **B**   | Sprint 3 statico         |
| `messages/Election`, `ElectionAck`               | **B**   | Sprint 3 (già stub)      |
| `messages/Synchronization`                       | **B**   | Sprint 3 (già stub)      |
| `messages/ElectionAckTimeout`, `GlobalElectionTimeout` | **B** | Sprint 3 (già stub) |
| `src/test/.../election/` (unit test puri)        | **B**   | Sprint 3 statico         |
| `AbstractReplica.java`, `AbstractClient.java`    | **—**   | **codebase, INTOCCABILE** |
| `UpdateID`, `Update`, `UpdateHistory`            | **—**   | congelati (vedi §6)      |

> `AbstractReplica`/`AbstractClient` sono della codebase obbligatoria
> Genetti/Pasquali: **non si modificano**. Se serve un metodo lì dentro, si
> discute — quasi sempre la soluzione è un helper in `Replica` (A) o in
> `election/` (B).

---

## 2. Catalogo messaggi — CONGELATO

Firme definitive (già presenti in `src/main/java/it/unitn/ds/messages/`). Tutti
`Serializable` e immutabili. **Nessuno dei due li cambia** senza accordo.

| Messaggio             | Campi                                                        | Owner logica |
|-----------------------|-------------------------------------------------------------|--------------|
| `Heartbeat`           | *(nessuno)*                                                 | A            |
| `HeartbeatTimeout`    | *(nessuno)* — self-message                                  | A            |
| `UpdateTimeout`       | vedi stub — self-message replica su fase-1                  | A            |
| `ForwardTimeout`      | vedi stub — self-message replica dopo forward al coord      | A            |
| `Election`            | `int initiatorId`, `Map<Integer,UpdateID> latestPerReplica` | B            |
| `ElectionAck`         | *(nessuno)* — vedi decisione D1 in §7                       | B            |
| `ElectionAckTimeout`  | `int successorId` — self-message                            | B            |
| `GlobalElectionTimeout` | *(nessuno)* — self-message                                | B            |
| `Synchronization`     | `int newCoordinatorId`, `int newEpoch`, `List<Update> pendingUpdates` | B  |

Semantica congelata dei due messaggi non ovvi:

- **`Election.latestPerReplica`**: ogni replica che gestisce il messaggio
  inserisce la propria entry `id -> latestId` e forwarda al successore. Valore
  per una replica con history vuota = **`new UpdateID(0, 0)`** (sentinella
  "nessun update"; vedi decisione D2 in §7).
- **`Synchronization`**: inviato dal vincitore in broadcast. `pendingUpdates`
  = update orfani che ogni destinatario deve applicare *prima* di adottare
  `newEpoch`. `newEpoch` è calcolato dal vincitore (vedi decisione D3 in §7).

---

## 3. Enum `Crash.Type` — CONGELATO (già completo)

Definito in `AbstractReplica.Crash.Type`. **È già completo**, include il valore
`Election` che serve solo allo Sprint 3 → B non deve toccarlo, ed è comunque
nella codebase intoccabile.

```
Now, Heartbeat, Update, WriteOK, Election
```

Semantica (regola 10 codebase): `Crash(type, after_n_messages_of_type)` =
processa `n` messaggi di quel tipo, **crasha all'(n+1)-esimo**. `Now` = crash
immediato. Il conteggio per tipo è **interno ad A** (implementato in
`crash(...)` + `become(crashed())` nello Sprint 2); B non lo tocca, gli basta
sapere che `Election` è un punto di crash valido durante l'elezione.

---

## 4. Contratto della FSM (stati e `become`) — owner A

A implementa gli stati; B ne consuma solo i **nomi** e sa quali messaggi
ciascuno stato gestisce. Congeliamo i nomi dei behavior e la ripartizione dei
messaggi.

| Stato    | Metodo behavior        | Messaggi gestiti                                                                 |
|----------|------------------------|----------------------------------------------------------------------------------|
| `NORMAL` | `createReceive()`      | base (`Crash`,`InitSystem`) + `ClientRead/Write`, `ForwardWrite`, `UpdateMsg`, `UpdateAck`, `WriteOk`, **[S2]** `Heartbeat`, `HeartbeatTimeout`, `UpdateTimeout`, `ForwardTimeout` |
| `ELECTION` | `election()`         | `Crash` + `Election`, `ElectionAck`, `ElectionAckTimeout`, `GlobalElectionTimeout`, `Synchronization`; `Heartbeat` loggato/ignorato; write in ingresso **bufferizzate o droppate** (decisione D4 §7) |
| `CRASHED` | `crashed()`          | **nessuno** — `receiveBuilder().matchAny(ignore).build()`; **mai** `getContext().stop()` |

Regole congelate:

- Transizioni via `getContext().become(...)`: `NORMAL → CRASHED` (crash),
  `NORMAL → ELECTION` (heartbeat timeout / ricezione `Election`),
  `ELECTION → NORMAL` (processata `Synchronization`), `* → CRASHED` (crash).
- `crashed()` **non** parte da `createBaseReceiveBuilder()` (altrimenti
  ri-gestirebbe `Crash`): è un receive che ignora tutto.
- L'invio è **sempre** `this.tell(Serializable, ActorRef)` (canale FIFO
  emulato). **Mai** `getSelf().tell(...)` né `getContext().stop()`.

---

## 5. Contratto dei timeout — self-scheduled

Chi li schedula, con quale durata e cosa fanno. B possiede i due di elezione,
A gli altri; le firme sono già fissate in §2.

| Timeout                 | Schedulato da        | Durata indicativa                              | Owner |
|-------------------------|----------------------|------------------------------------------------|-------|
| `HeartbeatTimeout`      | replica non-coord    | `~3 × getCoordinatorBeatInterval()`            | A     |
| `UpdateTimeout`         | replica in fase-1    | `getMaxLatencyPlusTolerance()`                 | A     |
| `ForwardTimeout`        | replica dopo forward | `getMaxLatencyPlusTolerance()`                 | A     |
| `ElectionAckTimeout`    | replica che forwarda `Election` | `getMaxLatencyPlusTolerance()`      | B     |
| `GlobalElectionTimeout` | replica in `ELECTION`| `N × ElectionAckTimeout` (rete anti-livelock)  | B     |

Pattern condiviso: un timer per-scopo va tenuto in un `Cancellable` (o
`Map<UpdateID,Cancellable>` per i per-update) e **cancellato** quando arriva la
risposta attesa, per non far scattare falsi positivi.

---

## 6. API di confine che A espone e B consuma — CONGELATA

Sono i punti dove i due mondi si toccano. Le firme sono decise ora; i corpi
veri arrivano in Sprint 3.

### 6.1 Già esistenti e riusabili (nessun lavoro)

Da `UpdateHistory` (INTOCCABILE, già perfetto per lo scopo):

```java
Optional<UpdateID> latestId();       // per riempire Election.latestPerReplica
List<Update>       after(UpdateID t);// diff di Synchronization: già pronto!
List<Update>       asList();         // snapshot immutabile
```

Da `UpdateID`:

```java
int compareTo(UpdateID other);       // ordine lessicografico <epoch,seq>
UpdateID nextEpoch();                // primo id del nuovo epoch dopo elezione
```

Callback obbligatorie (in `AbstractReplica`, le chiama A/il codice di
integrazione — B ne conosce solo il *timing*, vedi §8):

```java
void callbackOnElectionStarted(int crashedCoordinatorId);
void callbackOnCoordinatorElected(int newCoordinatorId);
void callbackOnUpdateApplied(int index, int value);
```

### 6.2 Nuove classi di B — package `it.unitn.ds.election`

**Logica pura, senza attori, unit-testabile in isolamento.** Questi sono gli
**stub da committare in Fase 0** (corpi che lanciano `UnsupportedOperationException`
così il build è verde; B li riempie in Sprint 3). Firme **congelate**.

`src/main/java/it/unitn/ds/election/RingTopology.java`
```java
package it.unitn.ds.election;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Ring = replica ids in ascending order; successor = next id, skipping suspected. */
public final class RingTopology {
    private RingTopology() {}

    /** Members sorted ascending — the canonical ring order. */
    public static List<Integer> order(Collection<Integer> memberIds) {
        throw new UnsupportedOperationException("Sprint 3 — B");
    }

    /**
     * Next alive successor of {@code self} walking the ring, skipping every id
     * in {@code suspected}. Empty if nobody else is alive.
     */
    public static Optional<Integer> successor(int self,
                                              Collection<Integer> memberIds,
                                              Set<Integer> suspected) {
        throw new UnsupportedOperationException("Sprint 3 — B");
    }
}
```

`src/main/java/it/unitn/ds/election/ElectionLogic.java`
```java
package it.unitn.ds.election;

import java.util.Map;
import it.unitn.ds.UpdateID;

/** Winner selection from the Election message payload. */
public final class ElectionLogic {
    private ElectionLogic() {}

    /** Sentinel latest id for a replica with an empty history (decision D2). */
    public static final UpdateID NONE = new UpdateID(0, 0);

    /**
     * Winner = replica with the maximum latest {@link UpdateID} under natural
     * order; ties broken by the highest replica id.
     *
     * @param latestPerReplica id -> latestId (NONE for empty history)
     * @return the winning replica id
     */
    public static int winner(Map<Integer, UpdateID> latestPerReplica) {
        throw new UnsupportedOperationException("Sprint 3 — B");
    }
}
```

`src/main/java/it/unitn/ds/election/SyncPlan.java`
```java
package it.unitn.ds.election;

import java.util.List;
import it.unitn.ds.Update;
import it.unitn.ds.UpdateHistory;
import it.unitn.ds.UpdateID;

/** What the new coordinator must replay to each lagging replica. */
public final class SyncPlan {
    private SyncPlan() {}

    /**
     * Updates the winner must send in {@link
     * it.unitn.ds.messages.Synchronization#pendingUpdates} to a replica whose
     * latest known id is {@code recipientLatest}. Thin wrapper over
     * {@link UpdateHistory#after(UpdateID)} — kept as a named seam so the
     * election layer never reaches into history internals directly.
     */
    public static List<Update> missingFor(UpdateHistory winnerHistory, UpdateID recipientLatest) {
        throw new UnsupportedOperationException("Sprint 3 — B");
    }
}
```

### 6.3 Seam che A espone per il trigger (firma congelata, corpo in `[A+B]`)

In `Replica` (owner A), un metodo che l'integrazione chiamerà dal punto di
detection. B lo conosce per sapere *chi* avvia l'elezione:

```java
/** Enter ELECTION: build the Election payload with this replica's latestId
 *  and forward it to the ring successor. Fires callbackOnElectionStarted once. */
void startElection(int crashedCoordinatorId);   // A dichiara la firma; corpo in [A+B]
```

---

## 7. Decisioni da chiudere in Fase 0 (con raccomandazione)

Poche scelte aperte: decidetele **ora**, insieme, e annotate la scelta qui.

- **D1 — Correlazione di `ElectionAck`.** L'ack è attualmente vuoto. Con
  `GlobalElectionTimeout` che fa ripartire l'elezione, un ack stantìo potrebbe
  confondere.
  *Raccomandazione:* tenerlo vuoto (il ring è sequenziale e l'ack è hop-by-hop
  fra due vicini) e scartare ack inattesi nello stato `ELECTION`. Se in test
  emergono ambiguità, aggiungere `int initiatorId`. → **Scelta: ______**

- **D2 — Sentinella history vuota in `latestPerReplica`.**
  *Raccomandazione:* `ElectionLogic.NONE = new UpdateID(0,0)` (il primo update
  reale è `<0,1>` via `nextInEpoch()`, quindi `<0,0>` = "niente"). Congelato
  nello stub sopra. → **Scelta: ______**

- **D3 — Calcolo di `newEpoch`.** Il vincitore prima **completa gli update
  pendenti** nell'epoch corrente, poi bumpa.
  *Raccomandazione:* `newEpoch = maxEpochVisto + 1` (equivalente a
  `winnerLatestId.nextEpoch().epoch` dopo aver applicato gli orfani). → **Scelta: ______**

- **D4 — Write in arrivo durante `ELECTION`.**
  *Raccomandazione:* droppare (il client va in timeout e ritenta), niente buffer
  persistente in Sprint 3; rivalutare in Sprint 4 se un test lo richiede.
  → **Scelta: ______**

- **D5 — Sorgente del set `suspected` per il ring.** Chi è "noto crashed"?
  *Raccomandazione:* il coordinatore crashato (da `HeartbeatTimeout`) + ogni
  successore che non ha ACKato (`ElectionAckTimeout`). A mantiene il set, lo
  passa a `RingTopology.successor(...)`. → **Scelta: ______**

---

## 8. Timing delle callback — CONGELATO

Da rispettare in fase `[A+B]` (dai commenti in `AbstractReplica` e dai test):

- `callbackOnElectionStarted(crashedCoordId)` — **esattamente una volta per
  partecipazione all'elezione**, alla prima `Election` inviata da questa
  replica.
- `callbackOnCoordinatorElected(newCoordId)` — sul **vincitore** quando decide
  di aver vinto, e su **ogni altra replica** al processamento della
  `Synchronization`.
- `callbackOnUpdateApplied(idx, val)` — una volta per write applicata, anche
  per gli update orfani riapplicati durante la sync.

---

## 9. Exit criteria della Fase 0

Prima di aprire i due branch di lavoro:

- [ ] Le 3 classi stub in `election/` create con le firme di §6.2 (corpi
      `UnsupportedOperationException`).
- [ ] Gli stub dei test in `src/test/.../election/` creati (anche vuoti).
- [ ] Le 5 decisioni D1–D5 di §7 chiuse e annotate in questo file.
- [ ] `./gradlew build` **verde** da clone pulito.
- [ ] I test dello Sprint 1 restano verdi (`NoCrashes`, gli `APICompliance`
      dell'happy path) — la Fase 0 non deve regredire nulla.

A questo punto:

```
git checkout -b sprint2-detection   # branch di A
git checkout -b sprint3-election     # branch di B, dal medesimo commit di Fase 0
```

Entrambi i branch partono dallo **stesso commit** con contratto verde. Merge in
pair al "Punto di merge" descritto in `ROADMAP.md` → Sprint 3.
