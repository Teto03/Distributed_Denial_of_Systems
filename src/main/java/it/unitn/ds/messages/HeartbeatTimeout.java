package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Self-scheduled timeout fired by a non-coordinator replica when it has not
 * received any {@link Heartbeat} from the coordinator within a configured
 * window (typically a small multiple of {@code getCoordinatorBeatInterval()}).
 * Sprint 2 wires the detection, Sprint 3 hooks it to the election trigger.
 */
public final class HeartbeatTimeout implements Serializable {

    public HeartbeatTimeout() {}

    @Override
    public String toString() {
        return "HeartbeatTimeout";
    }
}
