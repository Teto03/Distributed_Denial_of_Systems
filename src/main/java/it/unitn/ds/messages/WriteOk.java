package it.unitn.ds.messages;

import java.io.Serializable;
import java.util.Objects;

import it.unitn.ds.UpdateID;

/**
 * Coordinator -> replicas: phase-2 broadcast that authorises every replica
 * to deliver the update identified by {@link #id}. On receipt the replica
 * applies the change to its positions array, appends to its
 * {@code UpdateHistory}, and invokes {@code callbackOnUpdateApplied}.
 */
public final class WriteOk implements Serializable {

    public final UpdateID id;

    public WriteOk(UpdateID id) {
        this.id = Objects.requireNonNull(id, "id must not be null");
    }

    @Override
    public String toString() {
        return "WriteOk(" + id + ")";
    }
}
