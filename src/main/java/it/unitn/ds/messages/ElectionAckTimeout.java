package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Self-scheduled timeout fired by the replica that just forwarded an
 * {@link Election} to its ring successor and did not see the matching
 * {@link ElectionAck}. On firing the sender drops the silent successor from
 * the ring and forwards the {@code Election} to the next replica.
 */
public final class ElectionAckTimeout implements Serializable {

    public final int successorId;

    public ElectionAckTimeout(int successorId) {
        this.successorId = successorId;
    }

    @Override
    public String toString() {
        return "ElectionAckTimeout(successor=" + successorId + ")";
    }
}
