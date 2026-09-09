package org.apache.seata.acta;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pure measurement, not a fix. Accumulates per-call-site nanosecond totals and
 * invocation counts for the Acta hook sites inside ConnectionProxyXA, so their
 * actual cost can be read off after a run instead of guessed at. bench-server
 * exposes {@link #snapshot()} on an HTTP endpoint; call {@link #reset()}
 * between runs.
 *
 * TOTAL sites count every invocation of the enclosing method, Acta-relevant or
 * not (the denominator for "how often is this site even reached"); the rest
 * are timed spans around one specific call, counted only when that call
 * actually happens (e.g. RESOLVE_IN_ID only when isPending() was true).
 */
public final class ActaTiming {

    private ActaTiming() {}

    public enum Site {
        /** Every ConnectionProxyXA.xaCommit() invocation, Acta-relevant or not -- the reach denominator. */
        XA_COMMIT_TOTAL,
        /** xaCommit()'s ActaService.isPending(tid) check, timed whenever Acta is installed. */
        XA_COMMIT_IS_PENDING,
        /** xaCommit()'s ActaService.resolveInId(tid) call, timed only when isPending(tid) was true. */
        XA_COMMIT_RESOLVE_IN_ID,
        /** xaCommit()'s ActaService.markPhase2Start() call, timed only when actaInId was resolved. */
        XA_COMMIT_MARK_PHASE2_START,
        /**
         * xaCommit()'s ActaService.markPhase2Done() call, timed only when actaInId was resolved.
         * Since markPhase2Done became asynchronous (paper 3.7: COMMITTED/ABORTED is off the
         * critical path) this measures the ENQUEUE, which is what the commit path actually pays.
         * The durable write it hands off is XA_COMMIT_MARK_PHASE2_DONE_ASYNC below.
         */
        XA_COMMIT_MARK_PHASE2_DONE,
        /**
         * The acta_meta write markPhase2Done(COMMIT) hands to the background executor, timed on
         * that executor's thread and counted once per enqueued write, failures included. Paired
         * with XA_COMMIT_MARK_PHASE2_DONE: the two counts stay equal, and the difference between
         * their averages is exactly what moving the write off the critical path bought.
         */
        XA_COMMIT_MARK_PHASE2_DONE_ASYNC,

        /** Every ConnectionProxyXA.xaRollback(xid, branchId, data) invocation, Acta-relevant or not -- the reach denominator. */
        XA_ROLLBACK_TOTAL,
        /** xaRollback()'s ActaService.isPending(tid) check, timed whenever Acta is installed. */
        XA_ROLLBACK_IS_PENDING,
        /** xaRollback()'s ActaService.resolveInId(tid) call, timed only when isPending(tid) was true. */
        XA_ROLLBACK_RESOLVE_IN_ID,
        /** xaRollback()'s ActaService.markPhase2Start() call, timed only when actaInId was resolved. */
        XA_ROLLBACK_MARK_PHASE2_START,
        /** xaRollback()'s ActaService.markPhase2Done() enqueue -- see XA_COMMIT_MARK_PHASE2_DONE. */
        XA_ROLLBACK_MARK_PHASE2_DONE,
        /** The acta_meta write markPhase2Done(ABORT) hands off -- see XA_COMMIT_MARK_PHASE2_DONE_ASYNC. */
        XA_ROLLBACK_MARK_PHASE2_DONE_ASYNC,

        /** Every ConnectionProxyXA.commit() invocation that reaches the Acta hook site, Acta-relevant or not -- the reach denominator. */
        COMMIT_TOTAL,
        /** commit()'s ActaService.recordProgress() call (Figure 2 lines 14-22), timed only when actaEnabled. */
        COMMIT_RECORD_PROGRESS,

        /**
         * One count per activation the RPC entry point runs (bench-server's
         * ActaController). This is the PER-HOP DENOMINATOR: every other site's
         * count divided by this one is "how many of these does one hop cost".
         * Counted by bench-server, not by this module, since only the entry
         * point knows what an activation is.
         */
        ACTIVATION_TOTAL,

        /**
         * Every ActaMetadata.atomic() or single() call: one acta_meta round
         * trip, i.e. one connection checkout from the DEDICATED metadata pool
         * plus either a begin/commit-wrapped block or one self-committing
         * statement. Instrumented centrally in those two methods rather
         * than at each call site because every inbox/outbox/epoch/watermark
         * operation goes through it, so count/ACTIVATION_TOTAL is the measured
         * "acta_meta round trips per hop" figure directly.
         */
        META_ATOMIC,

        /** Every ActaService.deliver() call -- the denominator for the two DELIVER_* sites below. */
        DELIVER_TOTAL,

        /**
         * deliver()'s XaOps.isPrepared(tid) call (Figure 4 line 79), timed per
         * attempt (deliver() may poll). Kept separate from META_ATOMIC on
         * purpose: this is the ONLY Acta query that hits the APPLICATION
         * database and its connection pool -- the same MySQL/PostgreSQL the
         * workload under test is using -- so if Acta's overhead is dominated by
         * contention with the workload rather than by metadata bookkeeping,
         * this is the site that shows it.
         */
        DELIVER_IS_PREPARED,

        /** deliver()'s transport.receive() call: the network hop plus the whole synchronous downstream activation it triggers. */
        DELIVER_TRANSPORT_RECEIVE
    }

    private static final Map<Site, AtomicLong> NANOS = new EnumMap<>(Site.class);
    private static final Map<Site, AtomicLong> COUNTS = new EnumMap<>(Site.class);

    static {
        for (Site s : Site.values()) {
            NANOS.put(s, new AtomicLong());
            COUNTS.put(s, new AtomicLong());
        }
    }

    /** Records one occurrence of {@code site}: accumulates elapsedNanos, increments its count. */
    public static void record(Site site, long elapsedNanos) {
        NANOS.get(site).addAndGet(elapsedNanos);
        COUNTS.get(site).incrementAndGet();
    }

    /** Increments count only, for sites with no timed span of their own (e.g. a TOTAL counter). */
    public static void count(Site site) {
        COUNTS.get(site).incrementAndGet();
    }

    /** One row per site: {@code {count, totalNanos}}. */
    public static Map<String, long[]> snapshot() {
        Map<String, long[]> out = new LinkedHashMap<>();
        for (Site s : Site.values()) {
            out.put(s.name(), new long[] {COUNTS.get(s).get(), NANOS.get(s).get()});
        }
        return out;
    }

    public static void reset() {
        for (Site s : Site.values()) {
            NANOS.get(s).set(0);
            COUNTS.get(s).set(0);
        }
    }
}
