package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Coordinator -> replicas: periodic liveness signal. Defined in Sprint 1 for
 * completeness; the periodic emission and the {@code HeartbeatTimeout}
 * detection logic land in Sprint 2.
 */
public final class Heartbeat implements Serializable {

    public Heartbeat() {}

    @Override
    public String toString() {
        return "Heartbeat";
    }
}
