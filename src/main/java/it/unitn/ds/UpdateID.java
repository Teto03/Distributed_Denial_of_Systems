package it.unitn.ds;

import java.io.Serializable;
import java.util.Objects;

/**
 * Logical identifier of an update in the total order broadcast protocol.
 * It is a pair {@code <epoch, sequence>} where {@code epoch} is incremented
 * by each new coordinator after an election and {@code sequence} is the
 * monotonically increasing index of an update within an epoch.
 *
 * Ordering is lexicographic on {@code (epoch, sequence)}.
 */
public final class UpdateID implements Serializable, Comparable<UpdateID> {

    public final int epoch;
    public final int sequence;

    public UpdateID(int epoch, int sequence) {
        this.epoch = epoch;
        this.sequence = sequence;
    }

    /** Next update id in the same epoch (used by the coordinator when broadcasting). */
    public UpdateID nextInEpoch() {
        return new UpdateID(epoch, sequence + 1);
    }

    /** First update id of the next epoch (used after a successful election). */
    public UpdateID nextEpoch() {
        return new UpdateID(epoch + 1, 0);
    }

    @Override
    public int compareTo(UpdateID other) {
        int cmp = Integer.compare(this.epoch, other.epoch);
        return cmp != 0 ? cmp : Integer.compare(this.sequence, other.sequence);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UpdateID)) return false;
        UpdateID u = (UpdateID) o;
        return epoch == u.epoch && sequence == u.sequence;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, sequence);
    }

    @Override
    public String toString() {
        return "<" + epoch + "," + sequence + ">";
    }
}
