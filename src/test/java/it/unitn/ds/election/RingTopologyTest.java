package it.unitn.ds.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * Pure unit tests of the ring view used by the election protocol: no actor
 * system involved, only the ordering and the skip-on-suspicion rules.
 */
class RingTopologyTest {

    private static Set<Integer> suspected(Integer... ids) {
        return new HashSet<>(Arrays.asList(ids));
    }

    private static final Set<Integer> NOBODY = Collections.emptySet();

    // =================================================================================
    // order()
    // =================================================================================

    @Test
    void orderSortsIdsAscending() {
        assertEquals(Arrays.asList(0, 1, 2, 3, 4),
                RingTopology.order(Arrays.asList(3, 0, 4, 1, 2)));
    }

    @Test
    void orderIsStableOnAnAlreadySortedRing() {
        List<Integer> ids = Arrays.asList(0, 1, 2);
        assertEquals(ids, RingTopology.order(ids));
    }

    @Test
    void orderDeduplicatesRepeatedIds() {
        assertEquals(Arrays.asList(1, 2), RingTopology.order(Arrays.asList(2, 1, 2, 1)));
    }

    @Test
    void orderAcceptsTheKeySetOfTheGroupMap() {
        // This is how Replica will call it: group.keySet() of InitSystem.
        Map<Integer, String> group = new LinkedHashMap<>();
        group.put(2, "replica2");
        group.put(0, "replica0");
        group.put(1, "replica1");
        assertEquals(Arrays.asList(0, 1, 2), RingTopology.order(group.keySet()));
    }

    @Test
    void orderOfAnEmptyMembershipIsEmpty() {
        assertTrue(RingTopology.order(Collections.emptyList()).isEmpty());
    }

    @Test
    void orderReturnsAnUnmodifiableSnapshot() {
        List<Integer> source = new ArrayList<>(Arrays.asList(0, 1, 2));
        List<Integer> ring = RingTopology.order(source);

        assertThrows(UnsupportedOperationException.class, () -> ring.add(3));

        // A later change of the source collection must not alter the ring.
        source.add(3);
        assertEquals(Arrays.asList(0, 1, 2), ring);
    }

    @Test
    void orderRejectsNullMembership() {
        assertThrows(NullPointerException.class, () -> RingTopology.order(null));
    }

    // =================================================================================
    // successor() — no crashes
    // =================================================================================

    @Test
    void successorIsTheNextIdInAscendingOrder() {
        List<Integer> ring = Arrays.asList(0, 1, 2, 3, 4);
        assertEquals(Optional.of(3), RingTopology.successor(2, ring, NOBODY));
    }

    @Test
    void successorWrapsAroundAfterTheLastId() {
        List<Integer> ring = Arrays.asList(0, 1, 2, 3, 4);
        assertEquals(Optional.of(0), RingTopology.successor(4, ring, NOBODY));
    }

    @Test
    void successorWorksOnNonContiguousIds() {
        List<Integer> ring = Arrays.asList(3, 8, 17);
        assertEquals(Optional.of(8), RingTopology.successor(3, ring, NOBODY));
        assertEquals(Optional.of(17), RingTopology.successor(8, ring, NOBODY));
        assertEquals(Optional.of(3), RingTopology.successor(17, ring, NOBODY));
    }

    @Test
    void followingTheSuccessorVisitsEveryOtherReplicaExactlyOnce() {
        List<Integer> ring = Arrays.asList(0, 1, 2, 3, 4);
        List<Integer> visited = new ArrayList<>();

        int current = 2;
        for (int hop = 0; hop < ring.size(); hop++) {
            current = RingTopology.successor(current, ring, NOBODY).orElseThrow(AssertionError::new);
            visited.add(current);
        }

        // A full lap starting from 2 comes back to 2 as the last hop.
        assertEquals(Arrays.asList(3, 4, 0, 1, 2), visited);
    }

    @Test
    void aReplicaIsNeverItsOwnSuccessor() {
        assertFalse(RingTopology.successor(1, Arrays.asList(0, 1, 2), NOBODY).equals(Optional.of(1)));
    }

    @Test
    void singleMemberRingHasNoSuccessor() {
        assertEquals(Optional.empty(), RingTopology.successor(0, Collections.singletonList(0), NOBODY));
    }

    @Test
    void emptyRingHasNoSuccessor() {
        assertEquals(Optional.empty(), RingTopology.successor(0, Collections.emptyList(), NOBODY));
    }

    // =================================================================================
    // successor() — skipping suspected replicas
    // =================================================================================

    @Test
    void successorSkipsASuspectedReplica() {
        List<Integer> ring = Arrays.asList(0, 1, 2, 3, 4);
        assertEquals(Optional.of(4), RingTopology.successor(2, ring, suspected(3)));
    }

    @Test
    void successorSkipsTwoConsecutiveSuspectedReplicas() {
        // Sprint 4, corner case 3: two consecutive ring nodes crash during the election.
        List<Integer> ring = Arrays.asList(0, 1, 2, 3, 4);
        assertEquals(Optional.of(0), RingTopology.successor(2, ring, suspected(3, 4)));
    }

    @Test
    void successorSkipsSuspectedReplicasAcrossTheWrapAround() {
        List<Integer> ring = Arrays.asList(0, 1, 2, 3, 4);
        assertEquals(Optional.of(1), RingTopology.successor(3, ring, suspected(4, 0)));
    }

    @Test
    void successorIsEmptyWhenEveryOtherReplicaIsSuspected() {
        List<Integer> ring = Arrays.asList(0, 1, 2);
        assertEquals(Optional.empty(), RingTopology.successor(1, ring, suspected(0, 2)));
    }

    @Test
    void suspectingSelfDoesNotChangeTheSuccessor() {
        // Defensive: the caller is alive by construction, its own id in the set is irrelevant.
        List<Integer> ring = Arrays.asList(0, 1, 2);
        assertEquals(Optional.of(2), RingTopology.successor(1, ring, suspected(1)));
    }

    @Test
    void suspectingAReplicaOutsideTheRingIsHarmless() {
        List<Integer> ring = Arrays.asList(0, 1, 2);
        assertEquals(Optional.of(2), RingTopology.successor(1, ring, suspected(99)));
    }

    @Test
    void successorOfAnIdThatIsNotInTheRingStartsFromItsInsertionPoint() {
        // Happens transiently if the crashed coordinator has been removed from
        // the local view: the walk must still be well defined.
        List<Integer> ring = Arrays.asList(0, 2, 4);
        assertEquals(Optional.of(2), RingTopology.successor(1, ring, NOBODY));
        assertEquals(Optional.of(0), RingTopology.successor(5, ring, NOBODY));
    }

    @Test
    void successorRejectsNullArguments() {
        assertThrows(NullPointerException.class,
                () -> RingTopology.successor(0, null, NOBODY));
        assertThrows(NullPointerException.class,
                () -> RingTopology.successor(0, Arrays.asList(0, 1), null));
    }
}
