package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Hop-by-hop acknowledgement of an {@link Election} message. The sender of an
 * {@code Election} starts an {@code ElectionAckTimeout} after forwarding; if
 * the timeout fires without receiving this ack, the sender skips the silent
 * successor and forwards to the next one in the ring.
 */
public final class ElectionAck implements Serializable {

    public ElectionAck() {}

    @Override
    public String toString() {
        return "ElectionAck";
    }
}
