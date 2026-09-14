package org.apache.seata.acta;

import javax.transaction.xa.XAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deterministic crash injection for the recovery correctness matrix
 * (CLAUDE.md Task 4). Recovery's guarantees can only be verified by actually
 * crashing mid-protocol -- a graceful shutdown rolls back the XA branch and
 * turns every after-prepare scenario into a before-prepare one (invariant 6)
 * -- so this exists purely to make that crash reproducible at an exact point
 * instead of relying on timing.
 *
 * Static-singleton, mirroring {@link ActaRuntime}: armed by bench-server's
 * test driver over HTTP (an endpoint calling {@link #arm}), consulted from
 * inside hooks that cannot be constructor-injected (the XA layer) or that
 * live in a different module than the arming endpoint (bench-server's own
 * business methods). One-shot by construction -- disarm() clears the static
 * fields, and a fresh JVM after the halt starts with them unset regardless,
 * so a restarted process can never immediately re-crash on the same arm.
 */
public final class ActaFailureInjector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActaFailureInjector.class);

    /** Where in the protocol to kill the process -- see CLAUDE.md's crash matrix notes. */
    public enum FaultPoint {
        /** Before the XA branch is prepared. Recovery must RE-EXECUTE (recoverInput: DISPATCH). */
        BEFORE_PREPARE,
        /**
         * After XA prepare succeeds, before any later step observes it. No
         * decision exists anywhere yet, so recovery must SKIP re-execution and
         * the workflow can only resolve via the TC's own timeout -- this is
         * the safety case, not the paper's resumption claim (see CLAUDE.md).
         */
        AFTER_PREPARE_BEFORE_VOTE,
        /**
         * After recordProgress durably persists the tid and this attempt's
         * outputs, before XA prepare. If prepare never completes, DB crash
         * recovery rolls the branch back but the ALREADY-DURABLE outbox row
         * (recorded via Acta's independent metadata DataSource, invariant 1)
         * survives with a now-stale tid. Recovery must re-execute AND
         * deliver()'s tid-mismatch guard must suppress that stale row while
         * only the fresh redispatch's own output reaches the target --
         * exercises invariant 3 directly, which BEFORE_PREPARE does not.
         */
        AFTER_OUTPUT_PERSIST_BEFORE_DELIVER,
        /**
         * Inside the TC-driven xaCommit(xid, branchId, data) overload, after
         * markPhase2Start(COMMIT) but before xaResource.commit() applies it.
         * The TC already holds a durable COMMIT decision at this point (this
         * overload only runs because the TC drove it here); recovery must
         * SKIP (phase2 already set) and the TC's own RetryCommitting scan --
         * not any application thread -- redelivers the decision once this
         * process reconnects. This is what the paper's resumption claim
         * (§1, Figure 1c) actually requires: completion without redoing work
         * or waiting out an end-to-end timeout.
         */
        AFTER_COMMIT_DECIDED_BEFORE_APPLY,
        /**
         * Same window as {@link #AFTER_COMMIT_DECIDED_BEFORE_APPLY}, mirrored
         * for the TC-driven xaRollback(xid, branchId, data) overload: after
         * markPhase2Start(ABORT), before the branch is actually rolled back.
         * The TC already holds a durable ABORT decision; recovery must SKIP,
         * and the TC's own RetryRollbacking scan completes it on reconnect.
         */
        AFTER_ABORT_DECIDED_BEFORE_APPLY,
        /**
         * Make {@code xaResource.prepare} FAIL for one branch. Unlike every
         * other point here this does not crash the process: it raises the
         * failure the real bug appeared as (a PostgreSQL SSI pivot detected at
         * PREPARE TRANSACTION), so the phase-1 failure path and its new
         * visibility marker can be exercised deterministically while the JVM
         * stays up to answer for it.
         *
         * One-shot by construction: {@link #maybeFailPrepare} disarms itself as
         * it fires, because a surviving process would otherwise fail every
         * subsequent prepare on that service.
         */
        PREPARE_FAILS
    }

    private static volatile String armedService;
    private static volatile FaultPoint armedPoint;

    /**
     * Test-only budget, in ms, for how long {@link ActaService#deliver} may keep
     * retrying a transport failure. -1 (default) is Acta's real behaviour:
     * retry indefinitely, so a workflow whose target is temporarily unavailable
     * RESUMES once it returns.
     *
     * Setting it to 0 is the Figure 1c BASELINE ARM: the same chain, the same
     * shapes, the same everything, with only the mechanism under test removed —
     * a stalled output is abandoned instead of resumed, so the workflow aborts
     * and its completed work is discarded. Comparing against Mode.GLOBAL_TXN
     * instead would confound workload with mechanism: different call graph,
     * different node count, different phase-2 cost.
     */
    private static volatile long deliverRetryBudgetMs = -1L;

    private ActaFailureInjector() {}

    public static void arm(String serviceId, FaultPoint point) {
        armedService = serviceId;
        armedPoint = point;
        LOGGER.info("Acta ActaFailureInjector armed service={} point={}", serviceId, point);
    }

    /** See deliverRetryBudgetMs. -1 restores Acta's real behaviour. */
    public static void setDeliverRetryBudgetMs(long budgetMs) {
        deliverRetryBudgetMs = budgetMs;
        LOGGER.info("Acta ActaFailureInjector deliverRetryBudgetMs={}", budgetMs);
    }

    /** True when deliver() has exhausted the test-only retry budget. */
    public static boolean deliverBudgetExhausted(long elapsedMs) {
        long budget = deliverRetryBudgetMs;
        return budget >= 0 && elapsedMs > budget;
    }

    public static void disarm() {
        armedService = null;
        armedPoint = null;
    }

    /**
     * No-op unless {@code serviceId}/{@code point} match the last {@link #arm}
     * call. On a match: halts the JVM immediately -- no shutdown hooks, no
     * cleanup, so volatile state (gNextSeq, gSettle, XaOps' live XA connection
     * map) is lost exactly as it would be in a real crash. Do NOT replace with
     * System.exit or any graceful stop; that would roll back the very branch
     * this exists to leave prepared.
     */
    /**
     * No-op unless {@code serviceId} is armed for {@link FaultPoint#PREPARE_FAILS}.
     * On a match, disarms and throws, so the caller's existing phase-1 failure
     * handling runs exactly as it would for a real prepare failure.
     */
    public static void maybeFailPrepare(String serviceId) throws XAException {
        if (armedPoint == FaultPoint.PREPARE_FAILS && serviceId.equals(armedService)) {
            LOGGER.info("Acta ActaFailureInjector firing service={} point={}", serviceId, FaultPoint.PREPARE_FAILS);
            disarm();
            throw new XAException("ActaFailureInjector: injected PREPARE failure for " + serviceId);
        }
    }

    public static void maybeCrash(String serviceId, FaultPoint point) {
        if (point == armedPoint && serviceId.equals(armedService)) {
            LOGGER.info("Acta ActaFailureInjector firing service={} point={}", serviceId, point);
            Runtime.getRuntime().halt(70);
        }
    }
}
