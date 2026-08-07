package it.unitn.ds.election;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import it.unitn.ds.Update;
import it.unitn.ds.UpdateHistory;
import it.unitn.ds.UpdateID;

/**
 * What the new coordinator must replay to each lagging replica.
 *
 * <p>This is the safety-critical half of the election: the project description
 * requires that <i>"if a replica a applies an update w, then all correct
 * replicas will eventually apply w"</i>, which the newly elected coordinator
 * enforces by re-sending, before starting its own epoch, every update the
 * others may have missed while the old coordinator was dying.</p>
 */
public final class SyncPlan {
    private SyncPlan() {}

    /**
     * Updates the winner must send in {@link
     * it.unitn.ds.messages.Synchronization#pendingUpdates} to a replica whose
     * latest known id is {@code recipientLatest}. Thin wrapper over
     * {@link UpdateHistory#after(UpdateID)} — kept as a named seam so the
     * election layer never reaches into history internals directly.
     */
    public static List<Update> missingFor(UpdateHistory winnerHistory, UpdateID recipientLatest) {
        Objects.requireNonNull(winnerHistory, "winnerHistory must not be null");
        Objects.requireNonNull(recipientLatest, "recipientLatest must not be null (use ElectionLogic.NONE)");
        return winnerHistory.after(recipientLatest);
    }

    /**
     * Updates to put in the single {@code Synchronization} broadcast so that
     * <em>every</em> participant of the election catches up: the diff computed
     * against the most lagging replica, i.e. the minimum of the latest ids
     * carried by the {@code Election} payload.
     *
     * <p>Sending one list to everybody instead of a tailored list per replica
     * keeps the announcement a real broadcast; replicas that already delivered
     * some of these updates simply skip them, which is safe because delivery
     * is idempotent on the {@link UpdateID} (integrity: a process delivers a
     * message at most once).</p>
     */
    public static List<Update> missingForAll(UpdateHistory winnerHistory,
                                             Map<Integer, UpdateID> latestPerReplica) {
        Objects.requireNonNull(winnerHistory, "winnerHistory must not be null");
        return missingFor(winnerHistory, oldest(latestPerReplica));
    }

    /**
     * The smallest latest id in the election payload — the watermark below
     * which every participant is known to be up to date.
     */
    public static UpdateID oldest(Map<Integer, UpdateID> latestPerReplica) {
        Objects.requireNonNull(latestPerReplica, "latestPerReplica must not be null");
        if (latestPerReplica.isEmpty()) {
            throw new IllegalArgumentException("cannot plan a synchronization out of an empty election payload");
        }
        UpdateID min = null;
        for (UpdateID latest : latestPerReplica.values()) {
            Objects.requireNonNull(latest, "latest id must not be null (use ElectionLogic.NONE)");
            if (min == null || latest.compareTo(min) < 0) {
                min = latest;
            }
        }
        return min;
    }
}
