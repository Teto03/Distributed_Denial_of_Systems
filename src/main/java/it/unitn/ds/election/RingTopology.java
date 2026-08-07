package it.unitn.ds.election;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Ring = replica ids in ascending order; successor = next id, skipping suspected.
 *
 * <p>The logical ring of the election protocol is defined by the replica
 * identifiers (project description, "Coordinator election": <i>"The logical
 * ring is defined by ordering replicas according to their identifiers"</i>).
 * The ring is therefore a derived, purely functional view of the static
 * membership: no state is kept here, every method is a pure function of its
 * arguments so that it can be unit tested without an actor system.</p>
 *
 * <p>Crash tolerance is expressed through the {@code suspected} set: a replica
 * whose {@code ElectionAckTimeout} fired adds the silent successor to that set
 * and asks for the next one, which implements the <i>"skips it, and forwards
 * the message to the following replica in the ring"</i> rule.</p>
 */
public final class RingTopology {
    private RingTopology() {}

    /** Members sorted ascending — the canonical ring order. */
    public static List<Integer> order(Collection<Integer> memberIds) {
        Objects.requireNonNull(memberIds, "memberIds must not be null");
        // TreeSet: ascending order plus deduplication, so that a group map's
        // keySet and a plain list of ids yield exactly the same ring.
        return Collections.unmodifiableList(new ArrayList<>(new TreeSet<>(memberIds)));
    }

    /**
     * Next alive successor of {@code self} walking the ring, skipping every id
     * in {@code suspected}. Empty if nobody else is alive.
     *
     * <p>The walk starts at the smallest id strictly greater than {@code self}
     * and wraps around to the smallest id of the ring, so {@code self} itself
     * is never returned: a replica is never its own successor, and a ring in
     * which every other member is suspected yields {@link Optional#empty()}.
     * {@code self} does not need to belong to {@code memberIds}; if it does
     * not, the walk simply starts from its insertion point.</p>
     */
    public static Optional<Integer> successor(int self,
                                              Collection<Integer> memberIds,
                                              Set<Integer> suspected) {
        Objects.requireNonNull(suspected, "suspected must not be null");
        for (int candidate : walkFrom(self, order(memberIds))) {
            if (!suspected.contains(candidate)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    /**
     * The ring visited in hop order starting after {@code self}: first the ids
     * greater than {@code self} in ascending order, then the ids smaller than
     * it (the wrap-around), {@code self} excluded.
     */
    private static List<Integer> walkFrom(int self, List<Integer> ring) {
        List<Integer> greater = new ArrayList<>();
        List<Integer> smaller = new ArrayList<>();
        for (int id : ring) {
            if (id > self) {
                greater.add(id);
            } else if (id < self) {
                smaller.add(id);
            }
        }
        greater.addAll(smaller);
        return greater;
    }
}
