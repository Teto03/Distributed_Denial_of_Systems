package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Replica -> client: answer to a {@link ClientRead}. The client turns this
 * into a {@code ReadResult} and fires {@code callbackOnReadResult}.
 *
 * <p>{@link #fromReplica} is the id of the replica the client originally
 * contacted, as required by the codebase rule on {@code ReadResult.fromReplica}.</p>
 */
public final class ReadReply implements Serializable {

    public final long reqId;
    public final int index;
    public final int value;
    public final int fromReplica;

    public ReadReply(long reqId, int index, int value, int fromReplica) {
        this.reqId = reqId;
        this.index = index;
        this.value = value;
        this.fromReplica = fromReplica;
    }

    @Override
    public String toString() {
        return "ReadReply(req=" + reqId + ", idx=" + index + ", val=" + value + ", from=" + fromReplica + ")";
    }
}
