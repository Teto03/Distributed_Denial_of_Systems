package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;

/**
 * Runs the demo scenarios of the protocol. Each demo builds its own actor
 * system from scratch, drives it from the outside with client requests and
 * crash commands, and shuts it down before the next one starts, so that the
 * log of a scenario can be read on its own.
 *
 * <pre>
 *   ./gradlew run                 all four scenarios, one after the other
 *   ./gradlew run --args="3"      only scenario 3
 * </pre>
 */
public class Main {

    private static final int N_REPLICAS = 5;
    private static final int COORDINATOR_ID = 0;

    // A read is answered straight away by the contacted replica, so a couple of
    // round trips are more than enough.
    private static final long READ_TIMEOUT = 8L * AbstractReplica.MAX_LATENCY * N_REPLICAS;

    // A write, on the other hand, may have to survive the death of the
    // coordinator: crash detection (3 beats) plus the election plus the replay
    // towards the new coordinator. 20 s leaves a comfortable margin.
    private static final long WRITE_TIMEOUT = 20_000L;

    public static void main(String[] args) throws Exception {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(false);

        String scenario = args.length > 0 ? args[0] : "all";
        switch (scenario) {
            case "1":
                happyPath();
                break;
            case "2":
                nonCoordinatorCrash();
                break;
            case "3":
                coordinatorCrash();
                break;
            case "4":
                partialWriteOk();
                break;
            case "all":
                happyPath();
                nonCoordinatorCrash();
                coordinatorCrash();
                partialWriteOk();
                break;
            default:
                note("unknown scenario '" + scenario + "': use 1, 2, 3, 4 or all");
                break;
        }
    }

    // =================================================================================
    // Scenarios
    // =================================================================================

    /**
     * Nothing crashes: three writes go through the two-phase broadcast and a
     * read gives back the last value written at that index.
     */
    private static void happyPath() throws Exception {
        banner("DEMO 1 - happy path, no crashes");
        note("what to look for: the three updates get consecutive ids <0,1> <0,2> <0,3>,");
        note("and every replica applies them in that same order");

        Cluster cluster = bootstrap("demo1", N_REPLICAS, COORDINATOR_ID);
        ActorRef client = clientOn(cluster, "client", 4);

        client.tell(new WriteRequest(0, 10), ActorRef.noSender());
        pause(400);
        client.tell(new WriteRequest(1, 20), ActorRef.noSender());
        pause(400);
        client.tell(new WriteRequest(0, 30), ActorRef.noSender());
        pause(400);

        note("reading back index 0: the value must be 30, the last write of the total order");
        client.tell(new ReadRequest(0), ActorRef.noSender());
        pause(500);

        shutdown(cluster);
    }

    /**
     * A replica that is not the coordinator dies. The quorum is still there, so
     * writes keep completing; only the client that insists on talking to the
     * dead replica is left with a timeout.
     */
    private static void nonCoordinatorCrash() throws Exception {
        banner("DEMO 2 - a non-coordinator replica crashes");
        note("what to look for: the write after the crash still reaches the quorum (3 acks out of 5),");
        note("while the client attached to the dead replica reports a READ timeout");

        Cluster cluster = bootstrap("demo2", N_REPLICAS, COORDINATOR_ID);
        ActorRef clientOnLive = clientOn(cluster, "client_live", 1);
        ActorRef clientOnDead = clientOn(cluster, "client_dead", 3);

        clientOnLive.tell(new WriteRequest(0, 10), ActorRef.noSender());
        pause(500);

        note("crashing replica 3");
        cluster.replicas.get(3).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        pause(300);

        clientOnLive.tell(new WriteRequest(0, 42), ActorRef.noSender());
        pause(600);

        note("the same request sent to the crashed replica gets no answer at all");
        clientOnDead.tell(new ReadRequest(0), ActorRef.noSender());
        pause(READ_TIMEOUT + 500);

        shutdown(cluster);
    }

    /**
     * The coordinator dies between two writes. The write issued right after the
     * crash is buffered by the contacted replica, the ring election picks a new
     * coordinator, and the buffered write is replayed in the new epoch.
     */
    private static void coordinatorCrash() throws Exception {
        banner("DEMO 3 - the coordinator crashes: ring election and synchronization");
        note("what to look for: HEARTBEAT/FORWARD timeout, the ELECTION going around the ring,");
        note("replica 4 winning the tie-break, and the buffered write completing in epoch 1");

        Cluster cluster = bootstrap("demo3", N_REPLICAS, COORDINATOR_ID);
        ActorRef client = clientOn(cluster, "client", 2);

        client.tell(new WriteRequest(0, 10), ActorRef.noSender());
        pause(500);

        note("crashing the coordinator (replica 0)");
        cluster.replicas.get(0).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        pause(300);

        note("this write has nowhere to go yet: replica 2 keeps it until the election is over");
        client.tell(new WriteRequest(0, 42), ActorRef.noSender());
        pause(8000);

        note("reading back index 0 from replica 2: the value must be 42");
        client.tell(new ReadRequest(0), ActorRef.noSender());
        pause(500);

        shutdown(cluster);
    }

    /**
     * The corner case the safety property is about: the coordinator dies with
     * the WRITEOK delivered to one replica only. That replica is the only one
     * that applied the update, it wins the election because it knows the most
     * recent one, and its SYNCHRONIZATION brings everybody else back in line.
     */
    private static void partialWriteOk() throws Exception {
        banner("DEMO 4 - the coordinator dies with the WRITEOK only partially disseminated");
        note("what to look for: replica 1 is the only one applying <0,1> before the crash,");
        note("it then wins the election and replays that update inside the SYNCHRONIZATION,");
        note("so an update observed by a single replica is not lost (uniform agreement)");

        Cluster cluster = bootstrap("demo4", N_REPLICAS, COORDINATOR_ID);
        ActorRef client = clientOn(cluster, "client", 4);

        note("arming the coordinator to die after having sent the WRITEOK to 2 replicas");
        cluster.replicas.get(0).tell(new Crash(Crash.Type.WriteOK, 2), ActorRef.noSender());
        pause(300);

        client.tell(new WriteRequest(0, 99), ActorRef.noSender());
        pause(10000);

        note("reading back index 0 from replica 4, which learned the value only from the sync");
        client.tell(new ReadRequest(0), ActorRef.noSender());
        pause(500);

        shutdown(cluster);
    }

    // =================================================================================
    // Helpers
    // =================================================================================

    /** A running actor system together with the replicas it holds. */
    private static final class Cluster {
        final ActorSystem system;
        final Map<Integer, ActorRef> replicas;

        Cluster(ActorSystem system, Map<Integer, ActorRef> replicas) {
            this.system = system;
            this.replicas = replicas;
        }
    }

    /** Create the replicas and hand them the initial view of the system. */
    private static Cluster bootstrap(String name, int nReplicas, int coordinatorId) {
        ActorSystem system = ActorSystem.create(name);

        Map<Integer, ActorRef> replicas = new HashMap<>(nReplicas);
        for (int i = 0; i < nReplicas; i++) {
            replicas.put(i, system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                            AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i));
        }

        InitSystem init = new InitSystem(replicas, coordinatorId);
        for (ActorRef replica : replicas.values()) {
            replica.tell(init, ActorRef.noSender());
        }

        note("system up: " + nReplicas + " replicas, coordinator is " + coordinatorId);
        return new Cluster(system, replicas);
    }

    /** A client whose requests go, by default, to the given replica. */
    private static ActorRef clientOn(Cluster cluster, String name, int replicaId) {
        return cluster.system.actorOf(
                Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.of(cluster.replicas.get(replicaId))),
                name);
    }

    private static void shutdown(Cluster cluster) throws Exception {
        cluster.system.terminate();
        cluster.system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        note("system down");
    }

    /** Give the scenario the time to unfold before the next step. */
    private static void pause(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private static void banner(String title) {
        Logger.log("==================================================================");
        Logger.log(title);
        Logger.log("==================================================================");
    }

    /** Commentary of the scenario, kept apart from the actors' own logs. */
    private static void note(String msg) {
        Logger.log("[demo] " + msg);
    }
}
