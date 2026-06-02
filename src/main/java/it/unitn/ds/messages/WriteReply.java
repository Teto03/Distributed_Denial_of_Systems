package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Replica -> client: confirmation that a write has been committed by the
 * system. Only the replica the client originally contacted sends it, so that
 * {@link #fromReplica} (and therefore {@code WriteResult.fromReplica}) carries
 * the contacted replica id rather than the coordinator one.
 */
public final class WriteReply implements Serializable {

    public final long reqId;
    public final int index;
    public final int value;
    public final int fromReplica;

    public WriteReply(long reqId, int index, int value, int fromReplica) {
        this.reqId = reqId;
        this.index = index;
        this.value = value;
        this.fromReplica = fromReplica;
    }

    @Override
    public String toString() {
        return "WriteReply(req=" + reqId + ", idx=" + index + ", val=" + value + ", from=" + fromReplica + ")";
    }
}
