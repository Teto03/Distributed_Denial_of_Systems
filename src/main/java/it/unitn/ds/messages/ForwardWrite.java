package it.unitn.ds.messages;

import java.io.Serializable;

import akka.actor.ActorRef;

/**
 * Non-coordinator replica -> coordinator: relay of a client {@code WriteRequest}
 * so the coordinator can assign an {@code <epoch, sequence>} id and start the
 * two-phase broadcast.
 *
 * <p>{@link #contactedReplicaId} preserves the id of the replica the client
 * originally addressed, so that the eventual {@code WriteResult} returned to
 * the client carries the right {@code fromReplica} (rule 11 of the mandatory
 * codebase). {@link #reqId} carries the client request identifier so the
 * contacted replica can later answer the right pending request.</p>
 */
public final class ForwardWrite implements Serializable {

    public final int index;
    public final int value;
    public final ActorRef client;
    public final int contactedReplicaId;
    public final long reqId;

    public ForwardWrite(int index, int value, ActorRef client, int contactedReplicaId, long reqId) {
        this.index = index;
        this.value = value;
        this.client = client;
        this.contactedReplicaId = contactedReplicaId;
        this.reqId = reqId;
    }

    @Override
    public String toString() {
        return "ForwardWrite(idx=" + index + ", val=" + value + ", contacted=" + contactedReplicaId + ")";
    }
}
