package it.unitn.ds.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import it.unitn.ds.Update;
import it.unitn.ds.UpdateHistory;
import it.unitn.ds.UpdateID;

/**
 * Pure unit tests of the synchronization diff: which updates the newly elected
 * coordinator has to replay so that no delivered update is ever lost.
 */
class SyncPlanTest {

    private static UpdateID id(int epoch, int sequence) {
        return new UpdateID(epoch, sequence);
    }

    private static Update update(int epoch, int sequence, int index, int value) {
        return new Update(id(epoch, sequence), index, value);
    }

    /** History of the winner used by most of the tests: <0,1>, <0,2>, <0,3>. */
    private static UpdateHistory winnerHistory() {
        UpdateHistory history = new UpdateHistory();
        history.append(update(0, 1, 0, 10));
        history.append(update(0, 2, 1, 20));
        history.append(update(0, 3, 2, 30));
        return history;
    }

    private static Map<Integer, UpdateID> payload(Object... idThenLatest) {
        Map<Integer, UpdateID> map = new HashMap<>();
        for (int i = 0; i < idThenLatest.length; i += 2) {
            map.put((Integer) idThenLatest[i], (UpdateID) idThenLatest[i + 1]);
        }
        return map;
    }

    // =================================================================================
    // missingFor()
    // =================================================================================

    @Test
    void aReplicaWithAnEmptyHistoryGetsTheWholeLog() {
        List<Update> missing = SyncPlan.missingFor(winnerHistory(), ElectionLogic.NONE);
        assertEquals(Arrays.asList(update(0, 1, 0, 10), update(0, 2, 1, 20), update(0, 3, 2, 30)), missing);
    }

    @Test
    void aLaggingReplicaGetsOnlyTheUpdatesItMissed() {
        List<Update> missing = SyncPlan.missingFor(winnerHistory(), id(0, 1));
        assertEquals(Arrays.asList(update(0, 2, 1, 20), update(0, 3, 2, 30)), missing);
    }

    @Test
    void anUpToDateReplicaGetsNothing() {
        assertTrue(SyncPlan.missingFor(winnerHistory(), id(0, 3)).isEmpty());
    }

    @Test
    void aReplicaAheadOfTheWinnerGetsNothing() {
        // Cannot happen for a real winner (it is the most up to date by
        // construction) but the diff must stay total, never negative.
        assertTrue(SyncPlan.missingFor(winnerHistory(), id(1, 0)).isEmpty());
    }

    @Test
    void theDiffPreservesTheTotalOrderOfTheLog() {
        List<Update> missing = SyncPlan.missingFor(winnerHistory(), ElectionLogic.NONE);
        for (int i = 1; i < missing.size(); i++) {
            assertTrue(missing.get(i - 1).id.compareTo(missing.get(i).id) < 0,
                    "updates must be replayed in increasing UpdateID order");
        }
    }

    @Test
    void theDiffSpansEpochBoundaries() {
        // Orphan update of the previous epoch plus the entries of the current one.
        UpdateHistory history = new UpdateHistory();
        history.append(update(0, 1, 0, 10));
        history.append(update(1, 1, 1, 20));
        history.append(update(1, 2, 2, 30));

        assertEquals(Arrays.asList(update(1, 1, 1, 20), update(1, 2, 2, 30)),
                SyncPlan.missingFor(history, id(0, 1)));
    }

    @Test
    void anEmptyWinnerHistoryHasNothingToReplay() {
        assertTrue(SyncPlan.missingFor(new UpdateHistory(), ElectionLogic.NONE).isEmpty());
    }

    @Test
    void theDiffIsAnUnmodifiableSnapshot() {
        List<Update> missing = SyncPlan.missingFor(winnerHistory(), ElectionLogic.NONE);
        assertThrows(UnsupportedOperationException.class, () -> missing.add(update(0, 4, 3, 40)));
    }

    @Test
    void missingForRejectsNullArguments() {
        assertThrows(NullPointerException.class, () -> SyncPlan.missingFor(null, ElectionLogic.NONE));
        assertThrows(NullPointerException.class, () -> SyncPlan.missingFor(winnerHistory(), null));
    }

    // =================================================================================
    // oldest() / missingForAll()
    // =================================================================================

    @Test
    void oldestIsTheMinimumLatestIdOfThePayload() {
        assertEquals(id(0, 1), SyncPlan.oldest(payload(
                0, id(0, 3),
                1, id(0, 1),
                2, id(0, 2))));
    }

    @Test
    void oldestComparesEpochsBeforeSequenceNumbers() {
        assertEquals(id(0, 9), SyncPlan.oldest(payload(
                0, id(1, 1),
                1, id(0, 9))));
    }

    @Test
    void theBroadcastCoversTheMostLaggingParticipant() {
        Map<Integer, UpdateID> latest = payload(
                0, id(0, 3),
                1, id(0, 1),
                2, id(0, 3));

        assertEquals(Arrays.asList(update(0, 2, 1, 20), update(0, 3, 2, 30)),
                SyncPlan.missingForAll(winnerHistory(), latest));
    }

    @Test
    void theBroadcastIsEmptyWhenEverybodyIsAlreadyAligned() {
        Map<Integer, UpdateID> latest = payload(
                0, id(0, 3),
                1, id(0, 3));
        assertTrue(SyncPlan.missingForAll(winnerHistory(), latest).isEmpty());
    }

    @Test
    void theBroadcastReplaysTheWholeLogWhenSomebodyHasAnEmptyHistory() {
        Map<Integer, UpdateID> latest = payload(
                0, id(0, 3),
                1, ElectionLogic.NONE);
        assertEquals(3, SyncPlan.missingForAll(winnerHistory(), latest).size());
    }

    @Test
    void planningOnAnEmptyPayloadIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SyncPlan.missingForAll(winnerHistory(), Collections.emptyMap()));
        assertThrows(IllegalArgumentException.class,
                () -> SyncPlan.oldest(Collections.emptyMap()));
    }

    @Test
    void oldestRejectsANullLatestId() {
        Map<Integer, UpdateID> latest = new HashMap<>();
        latest.put(0, id(0, 1));
        latest.put(1, null);
        assertThrows(NullPointerException.class, () -> SyncPlan.oldest(latest));
    }
}
