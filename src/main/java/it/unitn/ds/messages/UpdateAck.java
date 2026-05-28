package it.unitn.ds.messages;

import java.io.Serializable;
import java.util.Objects;

import it.unitn.ds.UpdateID;

/**
 * Replica -> coordinator: acknowledgement of a phase-1 {@link UpdateMsg}.
 * The coordinator counts ACKs per {@link UpdateID} until quorum
 * ({@code floor(N/2) + 1}) is reached, then issues the {@link WriteOk}.
 */
public final class UpdateAck implements Serializable {

    public final UpdateID id;

    public UpdateAck(UpdateID id) {
        this.id = Objects.requireNonNull(id, "id must not be null");
    }

    @Override
    public String toString() {
        return "UpdateAck(" + id + ")";
    }
}
