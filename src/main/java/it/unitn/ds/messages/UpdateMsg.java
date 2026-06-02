package it.unitn.ds.messages;

import java.io.Serializable;
import java.util.Objects;

import akka.actor.ActorRef;
import it.unitn.ds.Update;

/**
 * Coordinator -> replicas: phase-1 broadcast of an update proposal in the
 * two-phase total order protocol. Receivers reply with {@link UpdateAck}.
 *
 * <p>Wraps the immutable {@link Update} (id + index + value) and additionally
 * carries the originating {@link #client} reference together with
 * {@link #contactedReplicaId} and the client {@link #reqId}, so the replica
 * that was originally contacted can return the final {@code WriteResult} to
 * the client with the right {@code fromReplica} field once the update is
 * committed.</p>
 */
public final class UpdateMsg implements Serializable {

    public final Update update;
    public final ActorRef client;
    public final int contactedReplicaId;
    public final long reqId;

    public UpdateMsg(Update update, ActorRef client, int contactedReplicaId, long reqId) {
        this.update = Objects.requireNonNull(update, "update must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.contactedReplicaId = contactedReplicaId;
        this.reqId = reqId;
    }

    @Override
    public String toString() {
        return "UpdateMsg(" + update + ", contacted=" + contactedReplicaId + ")";
    }
}
