package it.unitn.ds.messages;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import it.unitn.ds.UpdateID;

/**
 * Ring election message. Each replica that handles the message records its
 * own latest known {@link UpdateID} into {@link #latestPerReplica} and
 * forwards to its ring successor.
 *
 * <p>The full election state machine is implemented in Sprint 3; the message
 * shape is fixed here so that earlier sprints can already reference it.</p>
 */
public final class Election implements Serializable {

    /** id of the replica that started this election (i.e. detected the coordinator crash) */
    public final int initiatorId;

    /** snapshot of the latest UpdateID seen by every replica visited so far */
    public final Map<Integer, UpdateID> latestPerReplica;

    public Election(int initiatorId, Map<Integer, UpdateID> latestPerReplica) {
        Objects.requireNonNull(latestPerReplica, "latestPerReplica must not be null");
        this.initiatorId = initiatorId;
        this.latestPerReplica = Collections.unmodifiableMap(new HashMap<>(latestPerReplica));
    }

    @Override
    public String toString() {
        return "Election(initiator=" + initiatorId + ", visited=" + latestPerReplica.keySet() + ")";
    }
}
