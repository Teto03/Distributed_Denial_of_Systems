# Distributed Systems 2025-2026 — Project

![Java](https://img.shields.io/badge/Java-21-ED8B00?style=flat-square&logo=java&logoColor=white)
![Akka](https://img.shields.io/badge/Akka-2.6-15A9CE?style=flat-square&logo=akka&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-9.2.1-02303A?style=flat-square&logo=gradle&logoColor=white)

**A Quorum-Based Total Order Broadcast Protocol for Distributed Intelligence Databases.**

A replicated integer-array store coordinated by a distinguished replica through a
two-phase total order broadcast (UPDATE → quorum of ACKs → WRITEOK).
When the coordinator crashes, a new one is elected via a ring-based algorithm
that picks the replica holding the most recent update (ties broken by the
highest replica ID). The new coordinator then completes any update that was
left pending by the previous epoch, preserving uniform agreement.

Implemented in **Java 21** with **Akka actors**.

## Repository layout

```
docs/      # specification, slides, planning notes
report/    # LaTeX report sources
src/       # Java sources (main + tests)
```

## Build and run

A Gradle wrapper at version **9.2.1** is used. To set it up the first time:

```bash
gradle wrapper --gradle-version 9.2.1
```

Then:

```bash
./gradlew build      # compile everything
./gradlew run        # launch the demo Main
./gradlew test       # run the supplied test suite
```

## Project sources

The Java implementation lives in `src/main/java/it/unitn/ds/`:

| File                 | Responsibility                                                 |
|----------------------|----------------------------------------------------------------|
| `AbstractReplica`    | base actor exposing the test API (do not modify)               |
| `AbstractClient`     | base actor exposing the test API (do not modify)               |
| `Replica`            | replica logic: read, write, election, synchronization          |
| `Client`             | client logic: send requests, manage read/write timeouts        |
| `NetworkChannel`     | FIFO channel actor with bounded random latency (do not modify) |
| `Logger`             | shared logging utility (do not modify)                         |
| `Main`               | demo entry point                                               |

The supplied JUnit tests are under `src/test/java/it/unitn/ds/base/`.

## Logging

All output goes through `Logger.log` / `Logger.debug`. Direct `System.out.println`
calls must be avoided: tests rely on strict timing windows and any unbuffered
print risks pushing the system out of those windows.

Each log line is prefixed with a millisecond-precision timestamp.

## Report

The PDF report (3-4 pages, max 6) is built from the LaTeX sources in `report/`.
