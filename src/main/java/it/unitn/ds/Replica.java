package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Cancellable;

import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import it.unitn.ds.messages.ClientRead;
import it.unitn.ds.messages.ClientWrite;
import it.unitn.ds.messages.ForwardWrite;
import it.unitn.ds.messages.ReadReply;
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
 * This class only covers the happy path (no crashes, no election); heartbeat
 * and crash handling come in Sprint 2, the ring election in Sprint 3.
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
    // Crash handling
    private AbstractReplica.Crash pendingCrash = null;
    private final Map<AbstractReplica.Crash.Type, Integer> messageCounters = new HashMap<>();

    // heartbeat tasks
    private Cancellable heartbeatTask;
    private Cancellable heartbeatTimeoutTask;

    // internal message for the coordinator to trigger a heartbeat broadcast
    private static class SendHeartbeatTick implements Serializable {
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

        // If instruction is to crash immediately (now) after 0 messages, do it.
        if (how_to_crash.type == AbstractReplica.Crash.Type.Now && how_to_crash.after_n_messages_of_type == 0) {
            triggerCrash();
        }
    }

    private void triggerCrash() {
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
            heartbeatTask = getContext().getSystem().scheduler().scheduleWithFixedDelay(
                    Duration.Zero(),
                    Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new SendHeartbeatTick(),
                    getContext().getSystem().dispatcher(),
                    getSelf());
        } else {
            resetHeartbeatTimeout();
        }
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ClientRead.class, this::onClientRead)
                .match(ClientWrite.class, this::onClientWrite)
                .match(ForwardWrite.class, this::onForwardWrite)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(UpdateAck.class, this::onUpdateAck)
                .match(WriteOk.class, this::onWriteOk)
                .match(SendHeartbeatTick.class, tick -> broadcast(new Heartbeat()))
                .match(Heartbeat.class, this::onHeartbeat)
                .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
                .match(ForwardTimeout.class, this::onForwardTimeout)
                .match(UpdateTimeout.class, this::onUpdateTimeout)
                .build();
    }

    // =================================================================================
    // Heartbeat behaviour
    // =================================================================================

    private void resetHeartbeatTimeout() {
        if (heartbeatTimeoutTask != null) {
            heartbeatTimeoutTask.cancel();
        }

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
        log("HEARTBEAT TIMEOUT: Coordinator " + coordinatorId + " is suspected to have crashed");
        // Start or ring election?
    }

    private void onForwardTimeout(ForwardTimeout msg) {
        forwardTimeouts.remove(msg.reqId);
        log("FORWARD TIMEOUT for req=" + msg.reqId + " idx=" + msg.index + " val=" + msg.value);
    }

    private void onUpdateTimeout(UpdateTimeout msg) {
        updateTimeouts.remove(msg.id);
        log("UPDATE TIMEOUT for " + msg.id);
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
        ActorRef client = getSender();
        if (isCoordinator()) {
            startUpdate(msg.index, msg.value, client, id, msg.reqId);
        } else {
            log("forwarding WRITE (idx=" + msg.index + ", val=" + msg.value + ") to coordinator " + coordinatorId);
            tell(new ForwardWrite(msg.index, msg.value, client, id, msg.reqId), group.get(coordinatorId));

            scheduleForwardTimeout(msg.reqId, msg.index, msg.value);
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
        broadcast(new UpdateMsg(update, client, contactedReplicaId, reqId));
    }

    /** Every replica acks a phase-1 proposal and remembers it until WRITEOK. */
    private void onUpdateMsg(UpdateMsg msg) {
        // Crash check and timeout reset
        if (!checkCrashCondition(AbstractReplica.Crash.Type.Update))
            return;
        if (!isCoordinator())
            resetHeartbeatTimeout();

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
            broadcast(new WriteOk(msg.id));
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

        UpdateMsg proposal = pendingUpdates.remove(msg.id);
        if (proposal == null) {
            // Either already delivered or never seen: nothing to do.
            return;
        }
        Update update = proposal.update;
        positions[update.index] = update.value;
        history.append(update);
        log("applied update " + update.id.epoch + ":" + update.id.sequence
                + " (" + update.index + ", " + update.value + ")");
        callbackOnUpdateApplied(update.index, update.value);

        if (proposal.contactedReplicaId == id) {
            tell(new WriteReply(proposal.reqId, update.index, update.value, id), proposal.client);
        }
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

    /** Send a message to every replica in the group (the coordinator included). */
    private void broadcast(Serializable msg) {
        for (ActorRef replica : group.values()) {
            tell(msg, replica);
        }
    }

    /** Cancels a scheduled timer, if it hasn't fired yet */
    private void cancelTimeout(Cancellable c) {
        if (c != null) {
            c.cancel();
        }
    }

    /**
     * (Re-)arms the ForwardTimeout for a forwarded write, cancelling any
     * previous timer for the same request first (same rationale as
     * resetHeartbeatTimeout: rescheduling onto the same variable without
     * cancelling leaves the old timer live).
     */
    private void scheduleForwardTimeout(long reqId, int index, int value) {
        cancelTimeout(forwardTimeouts.remove(reqId));
        Cancellable task = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(getCoordinatorBeatInterval() * 3L, TimeUnit.MILLISECONDS),
                getSelf(),
                new ForwardTimeout(reqId, index, value),
                getContext().getSystem().dispatcher(),
                getSelf());
        forwardTimeouts.put(reqId, task);
    }

    /**
     * (Re-)arms the UpdateTimeout for a phase-1 proposal we just ack'd,
     * cancelling any previous timer for the same update id first.
     */
    private void scheduleUpdateTimeout(UpdateID updateID) {
        cancelTimeout(updateTimeouts.remove(updateID));
        Cancellable task = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(getCoordinatorBeatInterval() * 3L, TimeUnit.MILLISECONDS),
                getSelf(),
                new UpdateTimeout(updateID),
                getContext().getSystem().dispatcher(),
                getSelf());
        updateTimeouts.put(updateID, task);
    }

    /**
     * Increments the counter for the given message type.
     * Returns true if the replica should process the message,
     * or false if the replica just crashed and should drop it.
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
