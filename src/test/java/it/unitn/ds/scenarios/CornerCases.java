package it.unitn.ds.scenarios;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * Corner cases of the fault model (sprint 4).
 * Every scenario keeps a strict majority of replicas alive, as the fault
 * model assumes.
 */
public class CornerCases {
    // A crash is armed from the outside; the replica notifies its listener as
    // soon as it receives the command, well before any protocol message moves.
    private static final Duration CRASH_ACK = Duration.ofMillis(500);

    /**
     * The coordinator dies after having sent the UPDATE to two replicas only.
     * The proposal never reaches a quorum of ACKs, so no WRITEOK is ever
     * issued and no replica delivers it: discarding it is the correct outcome,
     * because no client was ever told the write had succeded.
     *
     * What must not happen is the client request being lost with it. The
     * contacted replica keeps the request bufered while the election runs and
     * replays it to the new coordinator, so the write completes in the new
     * epoch and every correct replica converges to that value.
     */
    @Test
    void coordinatorCrashesDuringUpdateBroadcast() throws InterruptedException {
        final int N = 5;
        final int COORD = 0;
        final int CONTACTED = 4;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "cornerUpdateBroadcast", N, COORD);

        // Die after the UPDATE has been handed to 2 replicas out of 5.
        armCrash(sys, COORD, new Crash(Crash.Type.Update, 2));

        TestKit probe = new TestKit(sys.system);
        ActorRef client = clientOn(sys, probe, CONTACTED, "clientUpdateBroadcast");
        client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                ActorRef.noSender());

        // The write survives the death of the coordinator: it is replayed once
        // the ring election has produced a new one.
        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, CONTACTED), wr,
                "the buffered client write must complete in the new epoch");

        assertConverged(sys, survivorsExcept(N, COORD), TestsCommons.TEST_VALUE);

        sys.system.terminate();
    }

    /**
     * The coordinator dies after the WRITEOK has been handed to two
     * replicas: itself - so it never applies it - and replica 1. Replica 1 is
     * therefore the single replica in the whole system that delivered the
     * update. Being the one that knows the most recent update it wins the
     * election, and its SYNCHRONIZATION puts that update back into circulation
     * instead of letting it disappear with the old epoch.
     */
    @Test
    void coordinatorCrashesDuringWriteOkDissemination() throws InterruptedException {
        final int N = 5;
        final int COORD = 0;
        final int ONLY_APPLIER = 1;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "cornerWriteOkDissemination", N, COORD);

        // Die after the WRITEOK has been handed to 2 replicas: id 0 (itself,
        // which will not process it) and id 1.
        armCrash(sys, COORD, new Crash(Crash.Type.WriteOK, 2));

        TestKit probe = new TestKit(sys.system);
        ActorRef client = clientOn(sys, probe, ONLY_APPLIER, "clientWriteOk");
        client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                ActorRef.noSender());

        // The one replica that saw the WRITEOK is the one that answers the
        // client, so from the outside the write is simply succeded.
        probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                msg -> msg instanceof WriteResult);

        // It also knows the most recent update, so it must win the election.
        int elected = awaitAgreedCoordinator(sys, survivorsExcept(N, COORD));
        assertEquals(ONLY_APPLIER, elected, "the replica holding the most recent update must win the election");

        // Uniform agreement: an update delivered by one replica only is not lost.
        assertConverged(sys, survivorsExcept(N, COORD), TestsCommons.TEST_VALUE);

        sys.system.terminate();
    }

    /**
     * The ring must survive a hole of more than one node. Replicas 2 and 3 are
     * adjacent in the ring, so the replica that tries to forward the ELECTION
     * to 2 has to time out twice in a row - skipping 2, then 3 - before
     * reaching a live successor.
     *
     * N is 7 so that killing three replicas still leaves a strict majority
     * of four alive, as the fault model requires.
     */
    @Test
    void twoConsecutiveReplicasCrashDuringElection() throws InterruptedException {
        final int N = 7;
        final int COORD = 0;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "cornerConsecutiveRingCrashes", N, COORD);

        // The two ring neighbours go first and the coordinator last, so that
        // the holes are already there when the election starts: whoever
        // forwards to replica 2 is then guaranteed to time out twice in a row.
        // (Crashing them in the opposite order would race against the 3-beat
        // detection window and make the double skip only occasionally happen.)
        armCrash(sys, 2, new Crash(Crash.Type.Now, 0));
        armCrash(sys, 3, new Crash(Crash.Type.Now, 0));
        armCrash(sys, COORD, new Crash(Crash.Type.Now, 0));

        Set<Integer> survivors = new HashSet<>(Arrays.asList(1, 4, 5, 6));

        // The election terminates in spite of the two consecutive holes, and
        // everybody that surviced agrees on the same winner.
        int elected = awaitAgreedCoordinator(sys, survivors);
        assertTrue(survivors.contains(elected),
                "the new coordinator must be one of the surviving replicas, got " + elected);

        // And the system is usable again afterwards.
        TestKit probe = new TestKit(sys.system);
        ActorRef client = clientOn(sys, probe, 6, "clientConsecutive");
        client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                ActorRef.noSender());
        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, 6), wr,
                "writes must resume once the new coordinator has taken over");

        assertConverged(sys, survivors, TestsCommons.TEST_VALUE);

        sys.system.terminate();
    }

    /**
     * The winner of the election crashes before announcing itself.
     * Nobody would ever hear the outcome: the ELECTION message dies with the
     * winner. Only the {@code GlobalElectionTimeout} can unblock the system,
     * which is precisely what this test checks.
     */
    @Test
    void electionWinnerCrashesBeforeSynchronization() throws InterruptedException {
        final int N = 5;
        final int COORD = 0;
        final int PRESUMPTIVE_WINNER = 4;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "cornerWinnerDiesBeforeSync", N, COORD);

        // Armed first, so the condition is in place before any ELECTION moves.
        armCrash(sys, PRESUMPTIVE_WINNER, new Crash(Crash.Type.Election, 1));
        armCrash(sys, COORD, new Crash(Crash.Type.Now, 0));

        Set<Integer> survivors = new HashSet<>(Arrays.asList(1, 2, 3));

        // A second round has to be started from scratch, so allow for the extra
        // global-timeout window on top of the usual election budget.
        int elected = awaitAgreedCoordinator(sys, survivors,
                2 * TestsCommons.getElectionMaxDelay(sys));

        assertNotEquals(COORD, elected, "the crashed coordinator cannot be re-elected");
        assertNotEquals(PRESUMPTIVE_WINNER, elected,
                "the winner died before announcing itself, so it cannot be the coordinator");
        assertTrue(survivors.contains(elected),
                "the new coordinator must be one of the surviving replicas, got " + elected);

        sys.system.terminate();
    }

    /**
     * A non-coordinator replica acknowledges the UPDATE and then dies on the
     * WRITEOK, so it contributes its vote to the quorum but never applies the
     * value. The write must still complete - the quorum does not depend on any
     * single replica - and no election must be triggered, since the
     * coordinator is alive and nobody has any reason to suspect it.
     */
    @Test
    void replicaCrashesAfterAckBeforeApplying() throws InterruptedException {
        final int N = 5;
        final int COORD = 0;
        final int VICTIM = 2;
        final int CONTACTED = 4;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "cornerAckThenDie", N, COORD);

        // Crash on the arrival of the first WRITEOK: the ACK has already been
        // sent by then, the local delivery has not happened yet.
        armCrash(sys, VICTIM, new Crash(Crash.Type.WriteOK, 0));

        TestKit probe = new TestKit(sys.system);
        ActorRef client = clientOn(sys, probe, CONTACTED, "clientAckThenDie");
        client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                ActorRef.noSender());

        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, CONTACTED), wr,
                "the loss of one replica must not prevent the quorum from forming");

        // Everybody else, coordinator included, delivered the update.
        Set<Integer> alive = survivorsExcept(N, VICTIM);
        assertConverged(sys, alive, TestsCommons.TEST_VALUE);

        // The coordinator never stopped beating, so nobody had any reason tio
        // suspect it: the loss of a replica must not disturb the epoch.
        assertNoElectionStarted(sys, alive);

        sys.system.terminate();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Arm a crash and wait for the replica to confirm it took the command. */
    private static void armCrash(TestsSystemWrapper sys, int replicaId, Crash crash) {
        sys.actors.get(replicaId).tell(crash, ActorRef.noSender());
        sys.probes.get(replicaId).fishForMessage(CRASH_ACK, "Crash", msg -> msg instanceof Crash);
    }

    /** A client whose requests go, by default, to the given replica */
    private static ActorRef clientOn(TestsSystemWrapper sys, TestKit probe, int replicaId, String name) {
        return sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.ofNullable(sys.actors.get(replicaId)), probe.getRef()),
                name);
    }

    /** Every replica id of the system except the given one. */
    private static Set<Integer> survivorsExcept(int nNodes, int crashed) {
        Set<Integer> out = new HashSet<>();
        for (int i = 0; i < nNodes; i++) {
            if (i != crashed) {
                out.add(i);
            }
        }
        return out;
    }

    private static int awaitAgreedCoordinator(TestsSystemWrapper sys, Set<Integer> survivors) {
        return awaitAgreedCoordinator(sys, survivors, TestsCommons.getElectionMaxDelay(sys));
    }

    /**
     * Check that none of the given replicas ever started an election.
     * The probes are not empty - every delivered update has already pushed
     * an {@code updateApplied} onto them - so this cannot be expressed with
     * {@code expectNoMessage}. {@code fishForMessage} discards what does not
     * match, so it drains those events and times out exactly when no
     * {@code ElectionStarted} is present, which is the outcome we want.
     */
    private static void assertNoElectionStarted(TestsSystemWrapper sys, Set<Integer> replicaIds) {
        for (int replicaId : replicaIds) {
            boolean started = true;
            try {
                sys.probes.get(replicaId).fishForMessage(
                        Duration.ofMillis(300), "ElectionStarted",
                        msg -> msg instanceof ElectionStarted);
            } catch (AssertionError none) {
                started = false;
            }
            assertFalse(started,
                    "replica " + replicaId + " started an election although the coordinator was alive");
        }
    }

    /**
     * Wait for the surviving replicas to report a new coordinator and check
     * they all name the same one. A replica is allowed to stay silent - it may
     * have learned the outcome through a SYNCHRONIZATION that arrived while its
     * probe was already being drained - but at least one must speak, and no two
     * may disagree.
     */
    private static int awaitAgreedCoordinator(TestsSystemWrapper sys, Set<Integer> survivors, long windowMillis) {
        List<CoordinatorElected> received = new ArrayList<>();
        for (int replicaId : survivors) {
            try {
                received.add((CoordinatorElected) sys.probes.get(replicaId).fishForMessage(
                        Duration.ofMillis(windowMillis), "CoordinatorElected",
                        msg -> msg instanceof CoordinatorElected));
            } catch (AssertionError tolerated) {
                // This replica did not report within the window.
            }
        }

        assertTrue(!received.isEmpty(), "at least one replica must report a new coordinator");
        int elected = received.get(0).newCoordinatorId;
        for (CoordinatorElected ce : received) {
            assertEquals(elected, ce.newCoordinatorId,
                    "replica " + ce.replicaId + " disagrees on the identity of the new coordinator");
        }
        return elected;
    }

    /**
     * Read the index directly from each of the given replicas and check they
     * all hold {@code expected}. Reads are served locally, so this observes the
     * replicated state itself rather than what the protocol claims about it.
     */
    private static void assertConverged(TestsSystemWrapper sys, Set<Integer> replicaIds, int expected)
            throws InterruptedException {
        // Give the SYNCHRONIZATION the time to reach the laggards.
        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        for (int replicaId : replicaIds) {
            TestKit probe = new TestKit(sys.system);
            ActorRef reader = clientOn(sys, probe, replicaId, "reader_" + replicaId + "_" + System.nanoTime());
            reader.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), ActorRef.noSender());
            ReadResult rr = (ReadResult) probe.fishForMessage(
                    Duration.ofMillis(sys.client_read_timeout), "ReadResult",
                    msg -> msg instanceof ReadResult);
            assertEquals(expected, rr.value,
                    "replica " + replicaId + " did not converge to the agreed value");
        }
    }
}
