package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Client -> replica: request to write {@link #value} at position
 * {@link #index}. If the contacted replica is not the coordinator it relays
 * the request with a {@link ForwardWrite}; otherwise it starts the two-phase
 * broadcast directly.
 *
 * <p>{@link #reqId} travels with the write all the way to the {@code WriteOk}
 * so the contacted replica can return a {@link WriteReply} that the client is
 * able to match.</p>
 */
public final class ClientWrite implements Serializable {

    public final long reqId;
    public final int index;
    public final int value;

    public ClientWrite(long reqId, int index, int value) {
        this.reqId = reqId;
        this.index = index;
        this.value = value;
    }

    @Override
    public String toString() {
        return "ClientWrite(req=" + reqId + ", idx=" + index + ", val=" + value + ")";
    }
}
