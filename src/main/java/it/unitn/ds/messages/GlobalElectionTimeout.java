package it.unitn.ds.messages;

import java.io.Serializable;

/**
 * Self-scheduled safety net against election livelocks (Hint-2 of the
 * Italian planning guide, section 7). When this fires the replica restarts
 * a fresh election round from scratch, dropping any partial state from the
 * previous attempt.
 */
public final class GlobalElectionTimeout implements Serializable {

    public GlobalElectionTimeout() {}

    @Override
    public String toString() {
        return "GlobalElectionTimeout";
    }
}
