package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

import it.unitn.ds.messages.ClientRead;
import it.unitn.ds.messages.ClientWrite;
import it.unitn.ds.messages.ReadReply;
import it.unitn.ds.messages.WriteReply;

/**
 * External client of the replicated store. It sends read/write requests to a
 * replica and reports the outcome through the mandatory callbacks. Each request
 * is armed with a timeout: if the replica never answers, the corresponding
 * {@code ReadTimeout}/{@code WriteTimeout} is fired.
 */
public class Client extends AbstractClient {

    // Monotonic id used to tell concurrent requests apart.
    private long nextReqId = 0;

    // Request ids still waiting for an answer. When the reply arrives the id is
    // removed, so the scheduled timeout (if it fires later) is simply ignored.
    private final Set<Long> pending = new HashSet<>();

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        long reqId = nextReqId++;
        pending.add(reqId);
        log("requesting READ (" + index + ") to " + replica.path().name());
        replica.tell(new ClientRead(reqId, index), getSelf());
        schedule(getReadTimeoutDelay(), new ReadTick(reqId, replica, index));
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        long reqId = nextReqId++;
        pending.add(reqId);
        log("requesting WRITE (" + index + ", " + value + ") to " + replica.path().name());
        replica.tell(new ClientWrite(reqId, index, value), getSelf());
        schedule(getWriteTimeoutDelay(), new WriteTick(reqId, replica, index, value));
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadReply.class, this::onReadReply)
                .match(WriteReply.class, this::onWriteReply)
                .match(ReadTick.class, this::onReadTick)
                .match(WriteTick.class, this::onWriteTick)
                .build();
    }

    private void onReadReply(ReadReply msg) {
        if (pending.remove(msg.reqId)) {
            callbackOnReadResult(new ReadResult(true, msg.index, msg.value, msg.fromReplica));
        }
    }

    private void onWriteReply(WriteReply msg) {
        if (pending.remove(msg.reqId)) {
            callbackOnWriteResult(new WriteResult(true, msg.index, msg.value, msg.fromReplica));
        }
    }

    // A tick only fires the timeout if no reply has cleared the request yet.
    private void onReadTick(ReadTick tick) {
        if (pending.remove(tick.reqId)) {
            callbackOnReadTimeout(new ReadTimeout(getSelf(), tick.replica, tick.index));
        }
    }

    private void onWriteTick(WriteTick tick) {
        if (pending.remove(tick.reqId)) {
            callbackOnWriteTimeout(new WriteTimeout(getSelf(), tick.replica, tick.index, tick.value));
        }
    }

    /** Schedule a timeout message to ourselves after {@code delayMillis}. */
    private void schedule(long delayMillis, Object tick) {
        getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(delayMillis, TimeUnit.MILLISECONDS),
                getSelf(),
                tick,
                getContext().getSystem().dispatcher(),
                getSelf());
    }

    // =================================================================================
    // Internal timeout reminders (sent only to self)
    // =================================================================================

    private static final class ReadTick implements Serializable {
        final long reqId;
        final ActorRef replica;
        final int index;

        ReadTick(long reqId, ActorRef replica, int index) {
            this.reqId = reqId;
            this.replica = replica;
            this.index = index;
        }
    }

    private static final class WriteTick implements Serializable {
        final long reqId;
        final ActorRef replica;
        final int index;
        final int value;

        WriteTick(long reqId, ActorRef replica, int index, int value) {
            this.reqId = reqId;
            this.replica = replica;
            this.index = index;
            this.value = value;
        }
    }

}
