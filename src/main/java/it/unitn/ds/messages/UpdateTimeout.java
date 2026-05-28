package it.unitn.ds.messages;

import java.io.Serializable;
import java.util.Objects;

import it.unitn.ds.UpdateID;

/**
 * Self-scheduled timeout fired by a replica waiting for the {@link WriteOk}
 * of an update it has already acknowledged. If the {@code WriteOk} never
 * arrives the replica suspects the coordinator and (from Sprint 3 onwards)
 * starts the ring election.
 */
public final class UpdateTimeout implements Serializable {

    public final UpdateID id;

    public UpdateTimeout(UpdateID id) {
        this.id = Objects.requireNonNull(id, "id must not be null");
    }

    @Override
    public String toString() {
        return "UpdateTimeout(" + id + ")";
    }
}
