package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Self-scheduled timeout fired by a non-coordinator replica that has
 * forwarded a {@link ForwardWrite} to the coordinator and never observed the
 * matching {@link WriteOk}. Used in Sprint 1 to fail the client write with
 * {@code WriteTimeout}; from Sprint 3 onwards it also feeds suspicion of the
 * coordinator.
 */
public final class ForwardTimeout implements Serializable {

    public final int index;
    public final int value;

    public ForwardTimeout(int index, int value) {
        this.index = index;
        this.value = value;
    }

    @Override
    public String toString() {
        return "ForwardTimeout(idx=" + index + ", val=" + value + ")";
    }
}
