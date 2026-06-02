package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Client -> replica: request to read the value currently stored at
 * {@link #index}. The contacted replica answers locally with a
 * {@link ReadReply} (reads do not go through the two-phase protocol).
 *
 * <p>{@link #reqId} is a per-client identifier that lets the client match the
 * reply (or the timeout) back to the request that produced it.</p>
 */
public final class ClientRead implements Serializable {

    public final long reqId;
    public final int index;

    public ClientRead(long reqId, int index) {
        this.reqId = reqId;
        this.index = index;
    }

    @Override
    public String toString() {
        return "ClientRead(req=" + reqId + ", idx=" + index + ")";
    }
}
