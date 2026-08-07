package it.unitn.ds.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import it.unitn.ds.Update;
import it.unitn.ds.UpdateHistory;
import it.unitn.ds.UpdateID;

/**
 * Pure unit tests of the winner selection rule and of the helpers that build
 * the {@code Election} payload. No actor system involved.
 */
class ElectionLogicTest {

    private static UpdateID id(int epoch, int sequence) {
        return new UpdateID(epoch, sequence);
    }

    /** Convenience builder for the {@code latestPerReplica} payload. */
    private static Map<Integer, UpdateID> payload(Object... idThenLatest) {
        Map<Integer, UpdateID> map = new HashMap<>();
        for (int i = 0; i < idThenLatest.length; i += 2) {
            map.put((Integer) idThenLatest[i], (UpdateID) idThenLatest[i + 1]);
        }
        return map;
    }

    private static UpdateHistory historyOf(UpdateID... ids) {
        UpdateHistory history = new UpdateHistory();
        int index = 0;
        for (UpdateID uid : ids) {
            history.append(new Update(uid, index++, 42));
        }
        return history;
    }

    // =================================================================================
    // winner()
    // =================================================================================

    @Test
    void theOnlyParticipantWins() {
        assertEquals(3, ElectionLogic.winner(payload(3, id(0, 7))));
    }

    @Test
    void theMostUpToDateReplicaWins() {
        Map<Integer, UpdateID> latest = payload(
                0, id(0, 4),
                1, id(0, 9),
                2, id(0, 2));
        assertEquals(1, ElectionLogic.winner(latest));
    }

    @Test
    void aHigherEpochBeatsAHigherSequenceNumber() {
        // Lexicographic order on <epoch, sequence>: <1,0> is newer than <0,99>.
        Map<Integer, UpdateID> latest = payload(
                0, id(0, 99),
                1, id(1, 0));
        assertEquals(1, ElectionLogic.winner(latest));
    }

    @Test
    void tiesAreBrokenByTheHighestReplicaId() {
        Map<Integer, UpdateID> latest = payload(
                0, id(1, 5),
                4, id(1, 5),
                2, id(1, 5));
        assertEquals(4, ElectionLogic.winner(latest));
    }

    @Test
    void tieBreakOnlyAppliesAmongTheMostUpToDateReplicas() {
        // Replica 9 is the highest id but it is lagging: it must not win.
        Map<Integer, UpdateID> latest = payload(
                1, id(2, 3),
                5, id(2, 3),
                9, id(2, 2));
        assertEquals(5, ElectionLogic.winner(latest));
    }

    @Test
    void withEmptyHistoriesEverywhereTheHighestIdWins() {
        // Election right after start-up: nobody has delivered anything yet.
        Map<Integer, UpdateID> latest = payload(
                0, ElectionLogic.NONE,
                1, ElectionLogic.NONE,
                2, ElectionLogic.NONE);
        assertEquals(2, ElectionLogic.winner(latest));
    }

    @Test
    void anyDeliveredUpdateBeatsAnEmptyHistory() {
        Map<Integer, UpdateID> latest = payload(
                0, id(0, 1),
                7, ElectionLogic.NONE);
        assertEquals(0, ElectionLogic.winner(latest));
    }

    @Test
    void winnerDoesNotDependOnIterationOrder() {
        Map<Integer, UpdateID> ascending = new java.util.TreeMap<>(payload(
                0, id(1, 2),
                1, id(1, 2),
                2, id(0, 9)));
        Map<Integer, UpdateID> descending = new java.util.TreeMap<>(Collections.reverseOrder());
        descending.putAll(ascending);

        assertEquals(1, ElectionLogic.winner(ascending));
        assertEquals(1, ElectionLogic.winner(descending));
    }

    @Test
    void winnerOfAnEmptyPayloadIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ElectionLogic.winner(Collections.emptyMap()));
    }

    @Test
    void winnerRejectsANullLatestId() {
        Map<Integer, UpdateID> latest = new HashMap<>();
        latest.put(0, id(0, 1));
        latest.put(1, null);
        assertThrows(NullPointerException.class, () -> ElectionLogic.winner(latest));
    }

    // =================================================================================
    // latestOf()
    // =================================================================================

    @Test
    void latestOfAnEmptyHistoryIsTheSentinel() {
        assertEquals(ElectionLogic.NONE, ElectionLogic.latestOf(new UpdateHistory()));
    }

    @Test
    void theSentinelIsSmallerThanEveryRealUpdateId() {
        // The first real id assigned by a coordinator is <0,1> (nextInEpoch of <0,0>).
        assertTrue(ElectionLogic.NONE.compareTo(id(0, 1)) < 0);
    }

    @Test
    void latestOfANonEmptyHistoryIsTheLastAppendedId() {
        UpdateHistory history = historyOf(id(0, 1), id(0, 2), id(1, 1));
        assertEquals(id(1, 1), ElectionLogic.latestOf(history));
    }

    // =================================================================================
    // withEntry()
    // =================================================================================

    @Test
    void withEntryAddsTheOwnContributionToThePayload() {
        Map<Integer, UpdateID> forwarded = ElectionLogic.withEntry(payload(0, id(0, 3)), 1, id(0, 5));

        assertEquals(2, forwarded.size());
        assertEquals(id(0, 3), forwarded.get(0));
        assertEquals(id(0, 5), forwarded.get(1));
    }

    @Test
    void withEntryKeepsAnAlreadyRecordedContribution() {
        // Seeing our own id again means the message completed a ring lap: the
        // payload must stay the one every other replica has already seen.
        Map<Integer, UpdateID> forwarded = ElectionLogic.withEntry(payload(1, id(0, 5)), 1, id(0, 9));
        assertEquals(id(0, 5), forwarded.get(1));
    }

    @Test
    void withEntryDoesNotMutateTheSourcePayload() {
        Map<Integer, UpdateID> source = payload(0, id(0, 3));
        ElectionLogic.withEntry(source, 1, id(0, 5));
        assertEquals(1, source.size());
    }

    @Test
    void withEntryReturnsAnUnmodifiablePayload() {
        Map<Integer, UpdateID> forwarded = ElectionLogic.withEntry(Collections.emptyMap(), 0, ElectionLogic.NONE);
        assertThrows(UnsupportedOperationException.class, () -> forwarded.put(1, ElectionLogic.NONE));
    }

    @Test
    void anElectionStartedFromScratchCarriesJustTheInitiator() {
        Map<Integer, UpdateID> payload =
                ElectionLogic.withEntry(Collections.emptyMap(), 2, ElectionLogic.latestOf(historyOf(id(0, 4))));
        assertEquals(Collections.singletonMap(2, id(0, 4)), payload);
    }

    // =================================================================================
    // newEpoch()
    // =================================================================================

    @Test
    void newEpochIsOneMoreThanTheHighestEpochEverObserved() {
        Map<Integer, UpdateID> latest = payload(
                0, id(2, 7),
                1, id(1, 3),
                2, id(2, 1));
        assertEquals(3, ElectionLogic.newEpoch(latest));
    }

    @Test
    void firstElectionOnEmptyHistoriesStartsEpochOne() {
        // Epoch 0 is the one of the initial coordinator, so the successor starts at 1.
        Map<Integer, UpdateID> latest = payload(
                0, ElectionLogic.NONE,
                1, ElectionLogic.NONE);
        assertEquals(1, ElectionLogic.newEpoch(latest));
    }

    @Test
    void newEpochLooksAtEveryParticipantNotOnlyAtTheWinner() {
        // Replica 1 wins (highest id among the up-to-date ones) but replica 0
        // has seen epoch 4: reusing epoch 3 would break id uniqueness.
        Map<Integer, UpdateID> latest = payload(
                0, id(4, 1),
                1, id(4, 1),
                2, id(3, 9));
        assertEquals(1, ElectionLogic.winner(latest));
        assertEquals(5, ElectionLogic.newEpoch(latest));
    }

    @Test
    void newEpochOfAnEmptyPayloadIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ElectionLogic.newEpoch(Collections.emptyMap()));
    }
}
