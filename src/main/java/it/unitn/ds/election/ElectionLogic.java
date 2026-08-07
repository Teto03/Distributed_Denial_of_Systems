package it.unitn.ds.election;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import it.unitn.ds.UpdateHistory;
import it.unitn.ds.UpdateID;

/**
 * Winner selection from the Election message payload.
 *
 * <p>Project description, "Coordinator election": <i>"The elected coordinator
 * is therefore chosen as the replica that knows the most recent update;
 * replica identifiers are used to break ties when multiple replicas are
 * equally up to date."</i> The whole decision is a pure function of the
 * {@code latestPerReplica} map carried by the {@code Election} message, so
 * every replica that sees a full ring lap computes the very same winner
 * without any extra round of communication.</p>
 */
public final class ElectionLogic {
    private ElectionLogic() {}

    /** Sentinel latest id for a replica with an empty history (decision D2). */
    public static final UpdateID NONE = new UpdateID(0, 0);

    /**
     * Winner = replica with the maximum latest {@link UpdateID} under natural
     * order; ties broken by the highest replica id.
     *
     * @param latestPerReplica id -> latestId (NONE for empty history)
     * @return the winning replica id
     */
    public static int winner(Map<Integer, UpdateID> latestPerReplica) {
        Objects.requireNonNull(latestPerReplica, "latestPerReplica must not be null");
        if (latestPerReplica.isEmpty()) {
            throw new IllegalArgumentException("cannot elect a winner out of an empty election payload");
        }

        int winnerId = Integer.MIN_VALUE;
        UpdateID best = null;
        for (Map.Entry<Integer, UpdateID> e : latestPerReplica.entrySet()) {
            int candidateId = Objects.requireNonNull(e.getKey(), "replica id must not be null");
            UpdateID candidate = Objects.requireNonNull(e.getValue(),
                    "latest id of replica " + candidateId + " must not be null (use NONE for an empty history)");
            if (best == null) {
                winnerId = candidateId;
                best = candidate;
                continue;
            }
            int cmp = candidate.compareTo(best);
            if (cmp > 0 || (cmp == 0 && candidateId > winnerId)) {
                winnerId = candidateId;
                best = candidate;
            }
        }
        return winnerId;
    }

    /**
     * The value a replica must publish in {@code Election.latestPerReplica}:
     * its latest delivered {@link UpdateID}, or {@link #NONE} when it has not
     * delivered anything yet.
     */
    public static UpdateID latestOf(UpdateHistory history) {
        Objects.requireNonNull(history, "history must not be null");
        return history.latestId().orElse(NONE);
    }

    /**
     * The payload of the {@code Election} message to forward after this replica
     * has taken part in the round: a copy of {@code latestPerReplica} with the
     * entry of {@code replicaId} added.
     *
     * <p>An id already present is left untouched: the protocol says a replica
     * adds its information only <i>"if it is not already participating in the
     * election"</i>, so seeing our own id back means the message completed a
     * full ring lap and the payload must stay exactly the one every other
     * replica has seen. The result is unmodifiable and the input map is never
     * mutated, which keeps the message immutable as required.</p>
     */
    public static Map<Integer, UpdateID> withEntry(Map<Integer, UpdateID> latestPerReplica,
                                                   int replicaId,
                                                   UpdateID latest) {
        Objects.requireNonNull(latestPerReplica, "latestPerReplica must not be null");
        Objects.requireNonNull(latest, "latest must not be null (use NONE for an empty history)");
        Map<Integer, UpdateID> merged = new HashMap<>(latestPerReplica);
        merged.putIfAbsent(replicaId, latest);
        return Collections.unmodifiableMap(merged);
    }

    /**
     * Epoch the winner starts once it has completed the interrupted updates
     * (decision D3): one more than the highest epoch anybody in the election
     * has ever observed. Taking the maximum over the whole payload — and not
     * just over the winner's own history — guarantees the new epoch number is
     * strictly greater than every epoch already used in the system, so update
     * ids stay unique and monotonic across coordinator changes.
     */
    public static int newEpoch(Map<Integer, UpdateID> latestPerReplica) {
        Objects.requireNonNull(latestPerReplica, "latestPerReplica must not be null");
        if (latestPerReplica.isEmpty()) {
            throw new IllegalArgumentException("cannot compute the new epoch out of an empty election payload");
        }
        int maxEpoch = Integer.MIN_VALUE;
        for (UpdateID latest : latestPerReplica.values()) {
            Objects.requireNonNull(latest, "latest id must not be null (use NONE for an empty history)");
            maxEpoch = Math.max(maxEpoch, latest.epoch);
        }
        return maxEpoch + 1;
    }
}
