package it.unitn.ds;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Append-only log of {@link Update} entries that a replica has delivered.
 *
 * The history is the source of truth for two protocol-level decisions:
 * 1. Election winner choice — the replica whose {@link #latestId()} is the
 *    largest under {@link UpdateID}'s natural order wins (ties broken by
 *    replica id at the election layer).
 * 2. Synchronization — after an election, the new coordinator replays its
 *    most recent entries so that lagging replicas catch up (uniform
 *    agreement on whatever the previous coordinator had started).
 *
 * Logically immutable: entries can only be appended, never removed or
 * reordered. The instance itself is mutable for in-process bookkeeping;
 * callers that need a snapshot to send over the network should use
 * {@link #asList()}.
 */
public final class UpdateHistory implements Serializable {

    private final List<Update> updates = new ArrayList<>();

    public void append(Update u) {
        updates.add(Objects.requireNonNull(u, "Update must not be null"));
    }

    public int size() {
        return updates.size();
    }

    public boolean isEmpty() {
        return updates.isEmpty();
    }

    public Optional<Update> latest() {
        return updates.isEmpty() ? Optional.empty() : Optional.of(updates.get(updates.size() - 1));
    }

    public Optional<UpdateID> latestId() {
        return latest().map(u -> u.id);
    }

    public List<Update> asList() {
        return Collections.unmodifiableList(new ArrayList<>(updates));
    }

    /** Returns the entries strictly newer than {@code threshold}, in order. */
    public List<Update> after(UpdateID threshold) {
        List<Update> out = new ArrayList<>();
        for (Update u : updates) {
            if (u.id.compareTo(threshold) > 0) out.add(u);
        }
        return Collections.unmodifiableList(out);
    }

    @Override
    public String toString() {
        return "UpdateHistory(size=" + updates.size() + ", latest=" + latestId().orElse(null) + ")";
    }
}
