package it.unitn.ds.messages;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import it.unitn.ds.Update;

/**
 * Newly elected coordinator -> all replicas: announces the new coordinator
 * id and replays any pending updates that must be completed before the new
 * epoch starts. The list is filled by the winner with all updates strictly
 * newer than the recipient's latest known id (uniform agreement recovery).
 *
 * <p>Definition lives in Sprint 1 for completeness; the dispatch and the
 * receiver-side replay logic are implemented in Sprint 3.</p>
 */
public final class Synchronization implements Serializable {

    public final int newCoordinatorId;
    public final int newEpoch;
    public final List<Update> pendingUpdates;

    public Synchronization(int newCoordinatorId, int newEpoch, List<Update> pendingUpdates) {
        Objects.requireNonNull(pendingUpdates, "pendingUpdates must not be null");
        this.newCoordinatorId = newCoordinatorId;
        this.newEpoch = newEpoch;
        this.pendingUpdates = Collections.unmodifiableList(new ArrayList<>(pendingUpdates));
    }

    @Override
    public String toString() {
        return "Synchronization(newCoord=" + newCoordinatorId + ", newEpoch=" + newEpoch
                + ", pending=" + pendingUpdates.size() + ")";
    }
}
