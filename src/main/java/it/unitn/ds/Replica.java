package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Cancellable;

import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import it.unitn.ds.election.ElectionLogic;
import it.unitn.ds.election.RingTopology;
import it.unitn.ds.election.SyncPlan;
import it.unitn.ds.messages.ClientRead;
import it.unitn.ds.messages.ClientWrite;
import it.unitn.ds.messages.Election;
import it.unitn.ds.messages.ElectionAck;
import it.unitn.ds.messages.ElectionAckTimeout;
import it.unitn.ds.messages.ForwardWrite;
import it.unitn.ds.messages.GlobalElectionTimeout;
import it.unitn.ds.messages.ReadReply;
import it.unitn.ds.messages.Synchronization;
import it.unitn.ds.messages.UpdateAck;
import it.unitn.ds.messages.UpdateMsg;
import it.unitn.ds.messages.WriteOk;
import it.unitn.ds.messages.WriteReply;
import it.unitn.ds.messages.Heartbeat;
import it.unitn.ds.messages.HeartbeatTimeout;
import it.unitn.ds.messages.ForwardTimeout;
import it.unitn.ds.messages.UpdateTimeout;

/**
 * A replica of the shared integer array. Reads are served locally; writes go
 * through the coordinator with the two-phase total order broadcast
 * (UPDATE -> quorum of ACK -> WRITEOK).
 *
 * Crashes are emulated by switching to a behaviour that drops everything, and
 * the loss of the coordinator is recovered by the ring election followed by a
 * synchronization round.
 */
public class Replica extends AbstractReplica {

    // Replicated state: positions[i] is the value stored at index i.
    private final int[] positions = new int[POSITIONS_LIST_LENGTH];

    // System view, filled in initSystem().
    private Map<Integer, ActorRef> group;
    private int coordinatorId;

    // Local log of delivered updates (used by election/sync from Sprint 3 on).
    private final UpdateHistory history = new UpdateHistory();

    // --- Coordinator-only bookkeeping ---
    // Last <epoch, sequence> the coordinator has assigned. <0,0> means "none yet".
    private UpdateID lastAssignedId = new UpdateID(0, 0);
    // ACKs collected so far per update id.
    private final Map<UpdateID, Integer> ackCounts = new HashMap<>();
    // Ids that already reached the quorum, to avoid sending WRITEOK twice.
    private final Set<UpdateID> committed = new HashSet<>();

    // --- Every replica ---
    // Updates received in phase 1 and still waiting for their WRITEOK.
    private final Map<UpdateID, UpdateMsg> pendingUpdates = new HashMap<>();

    // Armed UpdateTimeout per update id (phase-1 ack -> WRITEOK window),
    // cancelled as soon as the matching WriteOk arrives.
    private final Map<UpdateID, Cancellable> updateTimeouts = new HashMap<>();
    // Armed ForwardTimeout per client request id (forwarded write -> UpdateMsg
    // window), cancelled as soon as the matching UpdateMsg arrives. Keyed by
    // reqId rather than (index, value): two different requests can target the
    // same pair (e.g. a client retry), so that alone is not a safe key.
    private final Map<Long, Cancellable> forwardTimeouts = new HashMap<>();

    // Alessandro
    // Client writes this replica was contacted for and has not answered yet.
    // The client never retries on its own, so a write whose coordinator died
    // mid-flight is replayed to the new coordinator once the election ends
    // (see replayPendingClientWrites). Insertion order is kept so that the
    // per-client FIFO order of requests survives the replay.
    private final Map<Long, PendingClientWrite> clientWrites = new LinkedHashMap<>();

    // Crash handling
    private AbstractReplica.Crash pendingCrash = null;
    private final Map<AbstractReplica.Crash.Type, Integer> messageCounters = new HashMap<>();

    // heartbeat tasks
    private Cancellable heartbeatTask;
    private Cancellable heartbeatTimeoutTask;

    // Election state
    // Replicas known to be dead: the crashed coordinator plus every successor
    // that failed to ACK an Election. Crashed replicas never recover, so the
    // set only grows.
    private final Set<Integer> suspected = new HashSet<>();
    // True while this replica is in the election behaviour
    private boolean participating = false;
    // Guards callbackOnElectionStarted to at most one call per election, even
    // if the round is restarted by the GlobalElectionTimeout
    private boolean electionStartedFired = false;
    // Last election payload forwarded, replayed when a successor is skipped
    private Election lastForwardedElection = null;

    private Cancellable electionAckTimeoutTask;
    private Cancellable globalElectionTimeoutTask;

    // internal message for the coordinator to trigger a heartbeat broadcast
    private static class SendHeartbeatTick implements Serializable {
    }

    /** A client write awating its WriteReply, kept so it can be replayed */
    private static final class PendingClientWrite {
        final long reqId;
        final int index;
        final int value;
        final ActorRef client;

        PendingClientWrite(long reqId, int index, int value, ActorRef client) {
            this.reqId = reqId;
            this.index = index;
            this.value = value;
            this.client = client;
        }
    }

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
            ActorRef listener) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        return group == null ? 0 : group.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // Alessandro
        // Crash
        this.pendingCrash = how_to_crash;

        // Now means "stop right away", so there is no message type left to
        // count: after_n_messages_of_type is ignored on purpose, otherwise a
        // Crash(Now, n) with n > 0 would arm a condition nobody ever checks and
        // the replica would stay alive forever.
        if (how_to_crash.type == AbstractReplica.Crash.Type.Now) {
            triggerCrash();
        }
    }

    private void triggerCrash() {
        cancelAllTimers();
        getContext().become(crashed());
    }

    private Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {
                    // Silently drop all messages
                    // Do not call getContext().stop(getSelf()) as actor should not be stopped
                })
                .build();
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.group = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        log("initialised: N=" + group.size() + ", coordinator=" + coordinatorId);

        // begin ticks
        if (isCoordinator()) {
            startHeartbeatLoop();
        } else {
            resetHeartbeatTimeout();
        }
    }

    // =================================================================================
    // Behaviours
    // =================================================================================

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ClientRead.class, this::onClientRead)
                .match(ClientWrite.class, this::onClientWrite)
                .match(ForwardWrite.class, this::onForwardWrite)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(UpdateAck.class, this::onUpdateAck)
                .match(WriteOk.class, this::onWriteOk)
                .match(SendHeartbeatTick.class, tick -> broadcast(new Heartbeat(), AbstractReplica.Crash.Type.Heartbeat))
                .match(Heartbeat.class, this::onHeartbeat)
                .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
                .match(ForwardTimeout.class, this::onForwardTimeout)
                .match(UpdateTimeout.class, this::onUpdateTimeout)
                // A replica that has not detected the crash yet can be pulled
                // into a round, or learn its outcome, while still in NORMAL.
                .match(Election.class, this::onElection)
                .match(ElectionAck.class, this::onElectionAck)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(GlobalElectionTimeout.class, this::onGlobalElectionTimeout)
                .match(Synchronization.class, this::onSynchronization)
                .build();
    }

    /**
     * ELECTION. Built on the base builder so that {@code Crash} keeps going
     * through the mandatory wrapper (log + listener notification).
     *
     * <p>
     * Reads are still served locally — they never involve the coordinator.
     * Incoming client writes are parked, not dropped, and replayed to the new
     * coordinator when the round ends. Everything else of the dying epoch
     * ({@code UpdateMsg}, {@code WriteOk}, heartbeats and their timeouts) is
     * ignored: this is what freezes the latest known update of a candidate for
     * the whole duration of the round (Hint 1).
     * </p>
     */
    private Receive election() {
        return createBaseReceiveBuilder()
                .match(ClientRead.class, this::onClientRead)
                .match(ClientWrite.class, this::onClientWriteDuringElection)
                .match(Election.class, this::onElection)
                .match(ElectionAck.class, this::onElectionAck)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(GlobalElectionTimeout.class, this::onGlobalElectionTimeout)
                .match(Synchronization.class, this::onSynchronization)
                .matchAny(msg -> debug("ignored while in ELECTION: " + msg))
                .build();
    }

    // =================================================================================
    // Heartbeat behaviour
    // =================================================================================

    private void startHeartbeatLoop() {
        cancelTimeout(heartbeatTimeoutTask);
        heartbeatTimeoutTask = null;
        cancelTimeout(heartbeatTask);
        heartbeatTask = getContext().getSystem().scheduler().scheduleWithFixedDelay(
                Duration.Zero(),
                Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                getSelf(),
                new SendHeartbeatTick(),
                getContext().getSystem().dispatcher(),
                getSelf());
    }

    private void rearmHeartbeatTimeout() {
        cancelTimeout(heartbeatTask);
        heartbeatTask = null;
        resetHeartbeatTimeout();
    }

    private void resetHeartbeatTimeout() {
        cancelTimeout(heartbeatTimeoutTask);

        // Using multiple of 3
        heartbeatTimeoutTask = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(getCoordinatorBeatInterval() * 3L, TimeUnit.MILLISECONDS),
                getSelf(),
                new HeartbeatTimeout(),
                getContext().getSystem().dispatcher(),
                getSelf());
    }

    private void onHeartbeat(Heartbeat msg) {
        if (!checkCrashCondition(AbstractReplica.Crash.Type.Heartbeat))
            return;

        if (!isCoordinator()) {
            resetHeartbeatTimeout();
        }
    }

    private void onHeartbeatTimeout(HeartbeatTimeout msg) {
        if (isCoordinator())
            return;
        log("HEARTBEAT TIMEOUT: Coordinator " + coordinatorId + " is suspected to have crashed");
        startElection(coordinatorId);
    }

    private void onForwardTimeout(ForwardTimeout msg) {
        forwardTimeouts.remove(msg.reqId);
        if (isCoordinator())
            return;
        log("FORWARD TIMEOUT for req=" + msg.reqId + " idx=" + msg.index + " val=" + msg.value
                + ": coordinator " + coordinatorId + " did not start the broadcast");
        startElection(coordinatorId);
    }

    private void onUpdateTimeout(UpdateTimeout msg) {
        updateTimeouts.remove(msg.id);
        if (isCoordinator())
            return;
        log("UPDATE TIMEOUT for " + msg.id + ": no WRITEOK from coordinator " + coordinatorId);
        startElection(coordinatorId);
    }

    // =================================================================================
    // Reads
    // =================================================================================

    /** A read is served straight from the local copy of the array. */
    private void onClientRead(ClientRead msg) {
        ActorRef client = getSender();
        int value = positions[msg.index];
        log("READ idx=" + msg.index + " -> " + value);
        tell(new ReadReply(msg.reqId, msg.index, value, id), client);
    }

    // =================================================================================
    // Writes (two-phase broadcast)
    // =================================================================================

    /**
     * A client write reaches the replica it contacted. If we are the
     * coordinator we start the broadcast right away, otherwise we relay the
     * request to the coordinator keeping track of who must be answered.
     */
    private void onClientWrite(ClientWrite msg) {
        PendingClientWrite w = new PendingClientWrite(msg.reqId, msg.index, msg.value, getSender());
        clientWrites.put(w.reqId, w);
        submitClientWrite(w);
    }

    /** During an election there is no coordinator to forward to: park it */
    private void onClientWriteDuringElection(ClientWrite msg) {
        log("WRITE (idx=" + msg.index + ", val=" + msg.value + ") buffered: election is in progress");
        clientWrites.put(msg.reqId, new PendingClientWrite(msg.reqId, msg.index, msg.value, getSender()));
    }

    /** Send (or re-send) a client write towards the current coordinator */
    private void submitClientWrite(PendingClientWrite w) {
        if (isCoordinator()) {
            startUpdate(w.index, w.value, w.client, id, w.reqId);
        } else {
            log("forwarding WRITE (idx=" + w.index + ", val=" + w.value + ") to coordinator " + coordinatorId);
            tell(new ForwardWrite(w.index, w.value, w.client, id, w.reqId), group.get(coordinatorId));
            scheduleForwardTimeout(w.reqId, w.index, w.value);
        }
    }

    /**
     * Replay every client write still unanswered towards the coordinator that
     * has just been elected. Without this the request would be lost forever:
     * the client issues each request exactly once and only reports a timeout.
     */
    private void replayPendingClientWrites() {
        if (clientWrites.isEmpty())
            return;
        log("replaying " + clientWrites.size() + " buffered write(s) to coordinator " + coordinatorId);
        for (PendingClientWrite w : new ArrayList<>(clientWrites.values())) {
            submitClientWrite(w);
        }
    }

    /** Only the coordinator handles forwarded writes. */
    private void onForwardWrite(ForwardWrite msg) {
        if (!isCoordinator()) {
            return;
        }
        startUpdate(msg.index, msg.value, msg.client, msg.contactedReplicaId, msg.reqId);
    }

    /** Coordinator: assign the next id and broadcast the phase-1 proposal. */
    private void startUpdate(int index, int value, ActorRef client, int contactedReplicaId, long reqId) {
        lastAssignedId = lastAssignedId.nextInEpoch();
        Update update = new Update(lastAssignedId, index, value);
        ackCounts.put(lastAssignedId, 0);
        log("UPDATE proposed " + update);
        broadcast(new UpdateMsg(update, client, contactedReplicaId, reqId), AbstractReplica.Crash.Type.Update);
    }

    /** Every replica acks a phase-1 proposal and remembers it until WRITEOK. */
    private void onUpdateMsg(UpdateMsg msg) {
        // Crash check and timeout reset
        if (!checkCrashCondition(AbstractReplica.Crash.Type.Update))
            return;
        if (!isCoordinator())
            resetHeartbeatTimeout();

        // The coordinator is alive
        cancelTimeout(forwardTimeouts.remove(msg.reqId));

        pendingUpdates.put(msg.update.id, msg);
        log("ACK " + msg.update.id + " to coordinator " + coordinatorId);
        tell(new UpdateAck(msg.update.id), group.get(coordinatorId));

        scheduleUpdateTimeout(msg.update.id);
    }

    /** Coordinator: count acks, and on quorum tell everyone to commit. */
    private void onUpdateAck(UpdateAck msg) {
        if (!isCoordinator() || committed.contains(msg.id)) {
            return;
        }
        int count = ackCounts.merge(msg.id, 1, Integer::sum);
        if (count >= quorum()) {
            committed.add(msg.id);
            ackCounts.remove(msg.id);
            log("quorum reached for " + msg.id + " -> WRITEOK");
            broadcast(new WriteOk(msg.id), AbstractReplica.Crash.Type.WriteOK);
        }
    }

    /** Every replica delivers the update; the contacted one answers the client. */
    private void onWriteOk(WriteOk msg) {
        // Crash check and timeout reset
        if (!checkCrashCondition(AbstractReplica.Crash.Type.WriteOK))
            return;
        if (!isCoordinator())
            resetHeartbeatTimeout();

        cancelTimeout(updateTimeouts.remove(msg.id));

        UpdateMsg proposal = pendingUpdates.get(msg.id);
        if (proposal == null) {
            // Either already delivered or never seen: nothing to do.
            return;
        }
        applyUpdate(proposal.update);
    }

    /**
     * Deliver an update exactly once: write the array, append to the history,
     * fire the mandatory callback and, if we are the replica the client
     * contacted, answer it.
     */
    private void applyUpdate(Update u) {
        positions[u.index] = u.value;
        history.append(u);
        log("applied update " + u.id.epoch + ":" + u.id.sequence
                + " (" + u.index + ", " + u.value + ")");
        callbackOnUpdateApplied(u.index, u.value);

        UpdateMsg proposal = pendingUpdates.remove(u.id);
        cancelTimeout(updateTimeouts.remove(u.id));
        if (proposal != null && proposal.contactedReplicaId == id) {
            clientWrites.remove(proposal.reqId);
            cancelTimeout(forwardTimeouts.remove(proposal.reqId));
            tell(new WriteReply(proposal.reqId, u.index, u.value, id), proposal.client);
        }
    }

    /** True if {@code uid} has already been delivered (the history is ordered) */
    private boolean alreadyDelivered(UpdateID uid) {
        return history.latestId().map(latest -> uid.compareTo(latest) <= 0).orElse(false);
    }

    // =================================================================================
    // Election
    // =================================================================================

    /**
     * Enter ELECTION: build the Election payload with this replica's latestId
     * and forward it to the ring successor. Fires callbackOnElectionStarted once.
     */
    private void startElection(int crashedCoordinatorId) {
        if (participating || group == null)
            return;

        participating = true;
        suspected.add(crashedCoordinatorId);

        // While a candidate we must not suspect anybody on account of heartbeat
        cancelTimeout(heartbeatTimeoutTask);
        heartbeatTimeoutTask = null;

        getContext().become(election());
        fireElectionStartedOnce(crashedCoordinatorId);
        rearmGlobalElectionTimeout();

        Map<Integer, UpdateID> payload = ElectionLogic.withEntry(
                Collections.<Integer, UpdateID>emptyMap(), id, ElectionLogic.latestOf(history));
        forwardElection(new Election(id, payload));
    }

    /** callbackOnElectionSTarted must fire at most once per election */
    private void fireElectionStartedOnce(int crashedCoordinatorId) {
        if (!electionStartedFired) {
            electionStartedFired = true;
            callbackOnElectionStarted(crashedCoordinatorId);
        }
    }

    /** Hand the election to the next alive replica of the ring */
    private void forwardElection(Election msg) {
        lastForwardedElection = msg;
        Optional<Integer> successor = RingTopology.successor(id, group.keySet(), suspected);

        if (successor.isPresent()) {
            tell(msg, group.get(successor.get()));
            rearmElectionAckTimeout(successor.get());
        } else {
            // this replica is the only one left aliv
            log("no alive successor left in the ring, winning by default");
            becomeWinner(msg.latestPerReplica);
        }
    }

    private void onElection(Election msg) {
        // Check crash condition specifically for election
        if (!checkCrashCondition(AbstractReplica.Crash.Type.Election))
            return;

        // ack sender immediately
        tell(new ElectionAck(), getSender());

        if (isStaleElection(msg)) {
            debug("dropping stale election " + msg + ": " + coordinatorId + " is already our coordinator");
            if (isCoordinator()) {
                // The sender is still stuck in a round we havea lready won:
                // re-announce our leadership so that it can move on.
                tell(new Synchronization(id, lastAssignedId.epoch,
                        SyncPlan.missingForAll(history, msg.latestPerReplica)), getSender());
            }
            return;
        }

        if (!participating) {
            // Puleld into a round started by somebody else
            participating = true;
            suspected.add(coordinatorId);
            cancelTimeout(heartbeatTimeoutTask);
            heartbeatTimeoutTask = null;
            getContext().become(election());
            // The coordinator we still believe in is the one that crashed
            fireElectionStartedOnce(coordinatorId);
            rearmGlobalElectionTimeout();
        }

        if (msg.latestPerReplica.containsKey(id)) {
            // Our own entry is back: the message completed a full ring lap, so
            // the payload is final and every replica computes the same winner
            // out of it
            int winnerId = ElectionLogic.winner(msg.latestPerReplica);
            if (winnerId == id) {
                becomeWinner(msg.latestPerReplica);
            } else {
                // Keep it circulating: it must reach the winner, the only
                // replica allowed to announce the outcome
                log("ring lap complete, winner is " + winnerId + ": forwarding");
                forwardElection(msg);
            }
        } else {
            forwardElection(new Election(msg.initiatorId,
                    ElectionLogic.withEntry(msg.latestPerReplica, id, ElectionLogic.latestOf(history))));
        }
    }

    /**
     * A straggler from a round that has already been decided: it would elect
     * the coordinator we are already following, so re-entering ELECTION would
     * only restart a settled system.
     */
    private boolean isStaleElection(Election msg) {
        if (participating || msg.latestPerReplica.isEmpty() || suspected.contains(coordinatorId)) {
            return false;
        }
        return ElectionLogic.winner(msg.latestPerReplica) == coordinatorId;
    }

    // handle acks and timeouts
    private void onElectionAck(ElectionAck msg) {
        cancelTimeout(electionAckTimeoutTask);
        electionAckTimeoutTask = null;
    }

    private void onElectionAckTimeout(ElectionAckTimeout msg) {
        if (!participating || lastForwardedElection == null) {
            return;
        }

        // The successor did not answer: skip it and try the following one
        suspected.add(msg.successorId);
        log("ELECTION ACK TIMEOUT: suspecting " + msg.successorId + ", skipping it in the ring");
        forwardElection(lastForwardedElection);
    }

    /**
     * Anti-livelock net (Hint 2): if the round has not converged — typically
     * because the winner crashed right before announcing itself — start it
     * again from scratch. The callback is NOT fired again: this is still the
     * same election participation, just another attempt.
     */
    private void onGlobalElectionTimeout(GlobalElectionTimeout msg) {
        if (!participating)
            return;
        log("GLOBAL ELECTION TIMEOUT: restarting the election");
        cancelTimeout(electionAckTimeoutTask);
        electionAckTimeoutTask = null;
        lastForwardedElection = null;
        participating = false;
        startElection(coordinatorId);
    }

    private void rearmElectionAckTimeout(int successorId) {
        cancelTimeout(electionAckTimeoutTask);
        electionAckTimeoutTask = scheduleSelf(new ElectionAckTimeout(successorId), getMaxLatencyPlusTolerance());
    }

    private void rearmGlobalElectionTimeout() {
        cancelTimeout(globalElectionTimeoutTask);
        // needs to be long enough for a full ring lap
        long timeout = group.size() * getMaxLatencyPlusTolerance() * 2L;
        globalElectionTimeoutTask = scheduleSelf(new GlobalElectionTimeout(), timeout);
    }

    /**
     * This replica knows the most recent update: it takes over as coordinator.
     *
     * <p>
     * Order matters. The interrupted updates of the dead epoch are completed
     * <em>before</em> the new epoch is opened (Hint 3), so that the
     * {@code Synchronization} carries them and every correct replica converges
     * to the same state — this is what makes uniform agreement hold when the
     * old coordinator died after some replica had already applied an update.
     * </p>
     */
    private void becomeWinner(Map<Integer, UpdateID> latestPerReplica) {
        cancelTimeout(electionAckTimeoutTask);
        electionAckTimeoutTask = null;
        cancelTimeout(globalElectionTimeoutTask);
        globalElectionTimeoutTask = null;
        participating = false;
        electionStartedFired = false;
        lastForwardedElection = null;

        int newEpoch = ElectionLogic.newEpoch(latestPerReplica);
        log("WON the election, completing interrupted updates before epoch " + newEpoch);

        completeInterruptedUpdates();

        coordinatorId = id;
        lastAssignedId = new UpdateID(newEpoch, 0);
        ackCounts.clear();
        committed.clear();

        // One broadcast for everybody: the diff against the most lagging
        // participant. Replicas that already delivered part of it skip those
        // entries, delivery being idempotent on the UpdateID
        List<Update> missing = SyncPlan.missingForAll(history, latestPerReplica);
        Synchronization sync = new Synchronization(id, newEpoch, missing);
        for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
            if (entry.getKey() != id) {
                tell(sync, entry.getValue());
            }
        }

        getContext().become(createReceive());
        startHeartbeatLoop();
        callbackOnCoordinatorElected(id);
        replayPendingClientWrites();
    }

    /**
     * Updates this replica acknowledged in phase 1 but never saw confirmed:
     * the old coordinator may have died after applying them, or after sending
     * WRITEOK to somebody else. Delivering them now — and shipping them in the
     * Synchronization — is what prevents a client from having observed a value
     * that exists nowhere else.
     */
    private void completeInterruptedUpdates() {
        if (pendingUpdates.isEmpty())
            return;
        List<UpdateID> ids = new ArrayList<>(pendingUpdates.keySet());
        Collections.sort(ids);
        for (UpdateID uid : ids) {
            UpdateMsg proposal = pendingUpdates.get(uid);
            if (proposal == null || alreadyDelivered(uid)) {
                continue;
            }
            log("completing interrupted update " + uid);
            applyUpdate(proposal.update);
        }
        pendingUpdates.clear();
    }

    // Everybody learns the outcome
    private void onSynchronization(Synchronization msg) {
        if (!participating && msg.newCoordinatorId == coordinatorId && msg.newEpoch == lastAssignedId.epoch) {
            // We have already adopted this announcement. It can come back more
            // than once because the new coordinator answers every straggler
            // Election with a Synchronization, and running the handler again
            // would replay the buffered client writes a second time, turning
            // one client request into two updates.
            debug("ignoring a duplicate SYNCHRONIZATION from " + msg.newCoordinatorId);
            return;
        }

        cancelTimeout(electionAckTimeoutTask);
        electionAckTimeoutTask = null;
        cancelTimeout(globalElectionTimeoutTask);
        globalElectionTimeoutTask = null;
        participating = false;
        electionStartedFired = false;
        lastForwardedElection = null;

        coordinatorId = msg.newCoordinatorId;
        log("SYNCHRONIZATION from " + msg.newCoordinatorId + ": epoch " + msg.newEpoch
                + ", " + msg.pendingUpdates.size() + " update(s) to replay");

        // The list is ordered so a single pass is enough; entries we already
        // delivered are skipped
        for (Update u : msg.pendingUpdates) {
            if (!alreadyDelivered(u.id)) {
                applyUpdate(u);
            }
        }

        // Anything left over belongs to the dead epoch: from now on the new
        // coordinator is the only source of truth.
        for (Cancellable c : updateTimeouts.values()) {
            cancelTimeout(c);
        }
        updateTimeouts.clear();
        pendingUpdates.clear();
        ackCounts.clear();
        committed.clear();
        lastAssignedId = new UpdateID(msg.newEpoch, 0);

        getContext().become(createReceive());
        rearmHeartbeatTimeout();
        callbackOnCoordinatorElected(coordinatorId);
        replayPendingClientWrites();
    }

    // =================================================================================
    // Helpers
    // =================================================================================

    private boolean isCoordinator() {
        return id == coordinatorId;
    }

    /** Quorum size |Q| = floor(N/2) + 1. */
    private int quorum() {
        return group.size() / 2 + 1;
    }

    /**
     * Send a message to every replica in the group (the coordinator included),
     * walking the group in ascending id order.
     *
     * <p>
     * {@code crashPoint} is the crash type this broadcast exposes: the
     * condition is evaluated once per recipient, so a {@code Crash(type, n)}
     * makes the sender die after having served exactly {@code n} replicas and
     * leaves the others without the message. This is what emulates the
     * partially disseminated UPDATE / WRITEOK the project description asks to
     * be able to trigger; the receiving side of the same crash type is
     * unaffected, since a replica either broadcasts a given message (it is the
     * coordinator) or receives it, never both. Iterating in id order instead
     * of on the HashMap values keeps the set of replicas that were served
     * reproducible from one run to the next.
     * </p>
     */
    private void broadcast(Serializable msg, AbstractReplica.Crash.Type crashPoint) {
        for (int replicaId : RingTopology.order(group.keySet())) {
            if (!checkCrashCondition(crashPoint)) {
                log("crashed while broadcasting " + msg + ": the remaining replicas will not receive it");
                return;
            }
            tell(msg, group.get(replicaId));
        }
    }

    /** Cancels a scheduled timer, if it hasn't fired yet */
    private void cancelTimeout(Cancellable c) {
        if (c != null && !c.isCancelled()) {
            c.cancel();
        }
    }

    /** A crashed replica must stop sending, timers included. */
    private void cancelAllTimers() {
        cancelTimeout(heartbeatTask);
        heartbeatTask = null;
        cancelTimeout(heartbeatTimeoutTask);
        heartbeatTimeoutTask = null;
        cancelTimeout(electionAckTimeoutTask);
        electionAckTimeoutTask = null;
        cancelTimeout(globalElectionTimeoutTask);
        globalElectionTimeoutTask = null;
        for (Cancellable c : updateTimeouts.values()) {
            cancelTimeout(c);
        }
        updateTimeouts.clear();
        for (Cancellable c : forwardTimeouts.values()) {
            cancelTimeout(c);
        }
        forwardTimeouts.clear();
    }

    /**
     * (Re-)arms the ForwardTimeout for a forwarded write, cancelling any
     * previous timer for the same request first (same rationale as
     * resetHeartbeatTimeout: rescheduling onto the same variable without
     * cancelling leaves the old timer live).
     */
    private void scheduleForwardTimeout(long reqId, int index, int value) {
        cancelTimeout(forwardTimeouts.remove(reqId));
        forwardTimeouts.put(reqId,
                scheduleSelf(new ForwardTimeout(reqId, index, value), getCoordinatorBeatInterval() * 3L));
    }

    /**
     * (Re-)arms the UpdateTimeout for a phase-1 proposal we just ack'd,
     * cancelling any previous timer for the same update id first.
     */
    private void scheduleUpdateTimeout(UpdateID updateID) {
        cancelTimeout(updateTimeouts.remove(updateID));
        updateTimeouts.put(updateID,
                scheduleSelf(new UpdateTimeout(updateID), getCoordinatorBeatInterval() * 3L));
    }

    /** Schedule a self-message after {@code delayMillis} */
    private Cancellable scheduleSelf(Serializable msg, long delayMillis) {
        return getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(delayMillis, TimeUnit.MILLISECONDS),
                getSelf(),
                msg,
                getContext().getSystem().dispatcher(),
                getSelf());
    }

    /**
     * Increments the counter for the given message type.
     * Returns true if the replica should process the message,
     * or false if the replica just crashed and should drop it.
     *
     * <p>
     * The counter is shared between the two sides of the protocol: an incoming
     * message is counted when it is about to be processed (UPDATE, WRITEOK,
     * HEARTBEAT and ELECTION handlers), an outgoing one is counted once per
     * recipient inside {@link #broadcast(Serializable, Crash.Type)}. There is
     * no ambiguity because for any given type a replica is either the one
     * broadcasting it or one of the receivers. So Crash(Update, n) reads as
     * "die after having sent the UPDATE to n replicas" on the coordinator and
     * as "die after having processed n UPDATEs" on everybody else.
     * </p>
     */
    private boolean checkCrashCondition(AbstractReplica.Crash.Type type) {
        if (pendingCrash != null && pendingCrash.type == type) {
            int currentCount = messageCounters.getOrDefault(type, 0);
            if (currentCount >= pendingCrash.after_n_messages_of_type) {
                triggerCrash();
                return false;
            }
            messageCounters.put(type, currentCount + 1);
        }
        return true;
    }
}
