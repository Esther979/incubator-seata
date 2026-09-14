package org.apache.seata.acta;

import org.apache.seata.acta.ActaSpi.Handler;
import org.apache.seata.acta.ActaSpi.Transport;
import org.apache.seata.acta.ActaSpi.TwoPcClient;
import org.apache.seata.acta.ActaTypes.Decision;
import org.apache.seata.acta.ActaTypes.Delivery;
import org.apache.seata.acta.ActaTypes.InboxEntry;
import org.apache.seata.acta.ActaTypes.Message;
import org.apache.seata.acta.ActaTypes.NoVote;
import org.apache.seata.acta.ActaTypes.OutboxEntry;
import org.apache.seata.acta.ActaTypes.Phase2;
import org.apache.seata.acta.ActaTypes.Range;
import org.apache.seata.acta.ActaTypes.StreamPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Acta input processing and output delivery (paper Figures 2, 3, 4).
 *
 * The one invariant to keep in mind while reading this: an inbox entry's tid
 * is the ONLY link between persisted progress and the local subtransaction that
 * produced it. Progress is durable and immediately visible; its VALIDITY is
 * conditional on DB::isPrepared(tid). That is what sidesteps the dilemma in
 * §2.3 -- we get visibility without giving up atomicity.
 */
public final class ActaService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActaService.class);

    private final String serviceId;
    private final ActaMetadata meta;
    private final XaOps xa;
    private final TwoPcClient twoPc;
    private final Handler handler;
    private final Transport transport;

    /** Current incarnation's epoch, set by ActaRecovery.startIncarnation(). */
    private volatile long gEpoch = 0L;

    /** Volatile per-target sequence counters. Reset on every incarnation. */
    private final ConcurrentHashMap<String, AtomicLong> gNextSeq = new ConcurrentHashMap<>();

    /** Volatile per-target settle state. Reset on every incarnation. */
    private final ConcurrentHashMap<String, Settle> gSettle = new ConcurrentHashMap<>();

    private static final class Settle {
        final Object mutex = new Object();
        long end = 0L;
        final Map<Long, Long> ranges = new HashMap<>();
    }

    /**
     * In-memory tid -> input message id map for the tids Acta has recorded
     * progress for. It does two jobs:
     *
     *  - the pre-filter {@link #isPending} already did: ConnectionProxyXA's
     *    xaCommit()/xaRollback() run on Seata's RM callback thread for EVERY
     *    branch in the process, Acta-driven or not, so a branch Acta was never
     *    told about must cost a hash lookup, not an acta_meta round trip;
     *  - {@link #resolveInId}, which used to BE that round trip. Measured at
     *    concurrency 4, XA_COMMIT_RESOLVE_IN_ID was 8.1 ms per commit while
     *    pg_test_fsync puts one fsync at 0.17 ms, so the cost was JDBC round
     *    trips, not durability, and a map lookup removes it outright.
     *
     * The acta_meta lookup remains as the fallback on a miss, so correctness
     * never depends on this map being populated -- only speed does.
     *
     * Keyed on exactly the string resolveInId takes (the branch's
     * XAXid.toString()); recordProgress adds the identical value it is
     * called with, which ConnectionProxyXA.commit() sources from the same
     * xaBranchXid.toString(). A mismatched key format here would make every
     * lookup miss and silently disable phase-2 tracking, not just the
     * optimization. Concurrent because xaCommit/xaRollback run on RM threads
     * while recordProgress runs on request threads.
     */
    private final ConcurrentHashMap<String, String> inIdByTid = new ConcurrentHashMap<>();

    /**
     * The reverse direction, inId -> the tid CURRENTLY recorded for it. Not a
     * convenience: it is what keeps {@link #inIdByTid} faithful to the database.
     *
     * recordProgress OVERWRITES acta_inbox.tid, so after a re-execution the old
     * tid matches no row at all and getInboxByTid(oldTid) returns null. That is
     * precisely why a phase-2 callback arriving for a stale, rolled-back branch
     * marks nothing today -- the same "tid mismatch means not ours" rule
     * deliver() applies explicitly at Figure 4 line 78. A tid -> inId cache that
     * kept the superseded entry would instead answer that callback with a LIVE
     * inId and let it drive markPhase2Start on an activation that is currently
     * re-executing, turning a lookup miss into a wrong write. So every
     * recordProgress evicts the inId's previous tid, which restores the
     * "stale tid resolves to nothing" behaviour the database gave for free.
     */
    private final ConcurrentHashMap<String, String> tidByInId = new ConcurrentHashMap<>();

    /** How long {@link #shutdown()} waits for queued phase-2 writes before giving up. */
    private static final long PHASE2_DRAIN_TIMEOUT_S = 30L;

    /**
     * Single-threaded, per-service executor for the terminal COMMITTED/ABORTED
     * write (paper 3.7: those are off the critical path -- unlike
     * markPhase2Start's COMMITTING/ABORTING, which Figure 3 requires to be
     * durable BEFORE the DB call and therefore stays synchronous).
     *
     * Single-threaded on purpose, not for throughput: it keeps the writes for
     * one service in enqueue order and bounds the extra load this puts on the
     * metadata pool to one connection, which invariant 1 sizes for.
     *
     * <h2>Why losing a queued write is safe</h2>
     *
     * A crash drops whatever is still queued, leaving those entries at
     * COMMITTING/ABORTING rather than COMMITTED/ABORTED. That is a state
     * recovery already tolerates and is already reached by other means -- it is
     * exactly what a crash between markPhase2Start and the DB call leaves
     * behind, which is the AFTER_COMMIT_DECIDED_BEFORE_APPLY crash-matrix
     * scenario. recoverInput SKIPs any entry with a non-null phase2 (Figure 7
     * line 150: the coordinator has already decided, so recovery must not
     * re-execute), and the TC's own retry scan then redelivers branchCommit /
     * branchRollback, which runs the phase-2 path again and re-enqueues the
     * terminal write. So recoverAll never needs to know whether a previous
     * incarnation had writes in flight: they went away with the JVM, and the
     * state they would have produced is reached again by redelivery.
     *
     * The only thing a lost write costs is GC latency -- tryGcInbox collects
     * only terminal entries -- never correctness.
     */
    private final ExecutorService phase2DoneExecutor;

    public ActaService(
            String serviceId, ActaMetadata meta, XaOps xa, TwoPcClient twoPc, Handler handler, Transport transport) {
        this.serviceId = serviceId;
        this.meta = meta;
        this.xa = xa;
        this.twoPc = twoPc;
        this.handler = handler;
        this.transport = transport;
        this.phase2DoneExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "acta-phase2-done-" + serviceId);
            t.setDaemon(true);
            return t;
        });
        // Daemon threads do not keep the JVM alive, so a graceful shutdown needs
        // this to flush what is queued. A crash (ActaFailureInjector's
        // Runtime.halt, invariant 6) deliberately runs no hook at all -- see
        // phase2DoneExecutor's Javadoc for why the dropped writes are safe.
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "acta-phase2-done-drain-" + serviceId));
        primePendingTids();
    }

    /**
     * Stop accepting new phase-2 writes and drain the queued ones. Idempotent.
     * Registered as a JVM shutdown hook by the constructor; also safe to call
     * directly from an owning container's lifecycle callback.
     */
    public void shutdown() {
        phase2DoneExecutor.shutdown();
        try {
            if (!phase2DoneExecutor.awaitTermination(PHASE2_DRAIN_TIMEOUT_S, TimeUnit.SECONDS)) {
                LOGGER.warn(
                        "Acta phase-2 writes still queued for {} after {}s; the remaining entries stay at "
                                + "COMMITTING/ABORTING and are resolved by TC redelivery after restart",
                        serviceId,
                        PHASE2_DRAIN_TIMEOUT_S);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Figure 6 prerequisite for the maps above: after a crash, prepared
     * branches exist that the TC will still drive xaCommit/xaRollback for, but
     * both maps start empty on a fresh instance. Without this, a recovering
     * branch's phase-2 callback would look like "not Acta's", skip
     * markPhase2Done, and leave phase2 NULL forever -- tryGcInbox never
     * collects that entry. Runs from the constructor so there is no window
     * between construction and use where the maps could be consulted unprimed.
     *
     * Rebuilds inIdByTid from the same rows and under the same condition that
     * populated it before the crash: an inbox row carrying a tid, not yet in a
     * terminal phase2. The row IS the record recordProgress wrote, so priming
     * from it reproduces exactly the mapping the previous incarnation held.
     */
    private void primePendingTids() {
        List<InboxEntry> entries = meta.single(ActaMetadata::listInbox);
        for (InboxEntry e : entries) {
            if (e.tid != null && e.phase2 != Phase2.COMMITTED && e.phase2 != Phase2.ABORTED) {
                rememberTid(e.msg.id, e.tid);
            }
        }
    }

    /**
     * Record tid as the current tid for inId, evicting whatever tid was
     * previously recorded for that inId -- see {@link #tidByInId}'s Javadoc for
     * why that eviction is a correctness requirement and not housekeeping.
     */
    private void rememberTid(String inId, String tid) {
        String previous = tidByInId.put(inId, tid);
        if (previous != null && !previous.equals(tid)) {
            inIdByTid.remove(previous);
        }
        inIdByTid.put(tid, inId);
    }

    /**
     * Drop a tid that can receive no further phase-2 callback. Package-private
     * so ActaRecovery.tryGcInbox can call it after deleting an inbox row:
     * a deleted row cannot be found by getInboxByTid either, so leaving the
     * mapping behind would let the cache answer where the database would not.
     * Removes the reverse entry only if it still points at this tid, so a
     * concurrent re-execution's newer tid is never unmapped.
     */
    void forgetTid(String inId, String tid) {
        if (tid == null) {
            return;
        }
        inIdByTid.remove(tid);
        if (inId != null) {
            tidByInId.remove(inId, tid);
        }
    }

    // ------------------------------------------------ incarnation-local state

    void resetIncarnation(long epoch) {
        this.gEpoch = epoch;
        gNextSeq.clear();
        gSettle.clear();
    }

    public long epoch() {
        return gEpoch;
    }

    XaOps xa() {
        return xa;
    }

    /**
     * This service's OWN metadata store (paper §3.2: each service keeps its
     * Inbox/Outbox/Epoch in its own local database).
     *
     * Public so callers outside this package -- bench-server's ActaController
     * and ActaRecoveryDispatch -- can reach the correct store by asking the
     * service that owns it, rather than holding an injected ActaMetadata bean
     * and having to decide which one applies. That decision is exactly what
     * goes wrong silently: with per-service stores, a lookup sent to the wrong
     * store finds no row at all rather than the wrong row.
     */
    public ActaMetadata meta() {
        return meta;
    }

    TwoPcClient twoPc() {
        return twoPc;
    }

    String serviceId() {
        return serviceId;
    }

    Transport transport() {
        return transport;
    }

    /** Globally unique, never reused. */
    private String freshTxnId() {
        return serviceId + ":" + gEpoch + ":" + UUID.randomUUID();
    }

    // -------------------------------------------------- process (Figure 2)

    /**
     * Process one durable input. The dispatcher may call this concurrently for
     * DIFFERENT inputs, but never concurrently for the same input.
     */
    public void process(Message in) {
        // Line 2: use the input message id as a stable participant id.
        if (!twoPc.register(in.gtid, in.id)) {
            // Lines 3-6: coordinator already aborted; record ABORTED without
            // ever starting an XA transaction.
            meta.atomic(c -> {
                if (ActaMetadata.getInbox(c, in.id) != null) {
                    ActaMetadata.setPhase2(c, in.id, Phase2.ABORTED);
                }
                return null;
            });
            return;
        }

        // Lines 7-8.
        final String tid = freshTxnId();
        final Connection appConn;
        try {
            appConn = xa.beginTxn(tid);
        } catch (Exception e) {
            abortAndVote(in, tid);
            return;
        }

        // Line 9: execute the activation. Remote invocations are materialized
        // as messages but NOT transmitted yet.
        final List<Message> outMsgs;
        try {
            List<Message> produced = handler.execute(appConn, in);
            outMsgs = produced == null ? new ArrayList<>() : produced;
        } catch (Exception e) {
            // Lines 10-12: no fresh tid and no output is persisted.
            abortAndVote(in, tid);
            return;
        }

        // Line 13.
        List<Range> ranges = reservePositions(outMsgs);

        // Lines 14-22: atomically record the new tid and the COMPLETE output set.
        boolean ok;
        try {
            ok = recordProgress(in.id, tid, outMsgs);
        } catch (RuntimeException e) {
            settlePositions(ranges);
            xa.finishTxn(tid, Decision.ABORT);
            throw e;
        }

        // Line 23: positions are settled whether or not the atomic block took.
        settlePositions(ranges);

        // Lines 24-26.
        if (!ok) {
            xa.finishTxn(tid, Decision.ABORT);
            return;
        }

        // Lines 27-30.
        if (!xa.prepareTxn(tid)) {
            abortAndVote(in, tid);
            return;
        }
        twoPc.voteYes(in.gtid, in.id);
    }

    /**
     * Figure 2 lines 14-22 ONLY: atomically record the tid and insert the
     * complete output set. Does not register/beginTxn/execute/vote -- callers
     * (both {@link #process} and {@code ConnectionProxyXA.commit()}, which
     * hooks in after Sonata's dummy write and before {@code xaResource.prepare()})
     * own the rest of the activation. {@code outMsgs} must already have
     * positions assigned by {@link #reservePositions}.
     */
    public boolean recordProgress(String inId, String tid, List<Message> outMsgs) {
        boolean ok = meta.atomic(c -> {
            InboxEntry cur = ActaMetadata.getInbox(c, inId);
            if (cur == null || cur.phase2 == Phase2.ABORTING || cur.phase2 == Phase2.ABORTED) {
                return Boolean.FALSE;
            }
            ActaMetadata.setInboxTid(c, inId, tid);
            for (Message m : outMsgs) {
                ActaMetadata.insertOutbox(c, new OutboxEntry(m, inId, tid, Delivery.NEW));
            }
            return Boolean.TRUE;
        });
        if (ok) {
            // Marks this tid as Acta's, so xaCommit/xaRollback's pre-filter lets
            // its eventual phase-2 callback through, and records the tid -> inId
            // direction so resolveInId can answer it without a round trip.
            rememberTid(inId, tid);
        }
        return ok;
    }

    /**
     * Cheap in-memory check for ConnectionProxyXA.xaCommit()/xaRollback(): is
     * this tid one Acta has recorded progress for? A hash lookup, no
     * acta_meta round trip -- see pendingTids' Javadoc for why this exists.
     */
    public boolean isPending(String tid) {
        return inIdByTid.containsKey(tid);
    }

    /** VOTING mark before a no vote is sent. Paired with {@link #markNoVoted}. */
    public void markNoVoting(String inId) {
        meta.atomic(c -> {
            if (ActaMetadata.getInbox(c, inId) != null) {
                ActaMetadata.setNoVote(c, inId, NoVote.VOTING);
            }
            return null;
        });
    }

    /** VOTED mark after a no vote has been sent. Paired with {@link #markNoVoting}. */
    public void markNoVoted(String inId) {
        meta.atomic(c -> {
            if (ActaMetadata.getInbox(c, inId) != null) {
                ActaMetadata.setNoVote(c, inId, NoVote.VOTED);
            }
            return null;
        });
    }

    /**
     * Reverse lookup from tid to input message id, for callers that only know
     * the branch's XAXid (i.e. {@code ConnectionProxyXA.xaCommit()}/{@code xaRollback()},
     * which run on the RM's phase-2 callback thread, not the request thread
     * where ActaContext is bound).
     */
    public String resolveInId(String tid) {
        String cached = inIdByTid.get(tid);
        if (cached != null) {
            return cached;
        }
        // Miss: the map is a cache, never the source of truth. A miss is normal
        // for a branch Acta never recorded, and the acta_meta lookup is what
        // still answers correctly if the map were ever incomplete.
        InboxEntry e = meta.single(c -> ActaMetadata.getInboxByTid(c, tid));
        return e == null ? null : e.msg.id;
    }

    /**
     * Make sure this input is definitively aborted and the coordinator has been
     * told -- Figure 3 lines 26-27, for a caller that is NOT inside the XA branch.
     *
     * The receiving endpoint calls this when an activation body throws. In the
     * normal case there is nothing left to do: Spring has already rolled the
     * local transaction back, and ConnectionProxyXA.rollback() ran
     * markNoVoting -> reportStatusToTC(PhaseOne_Failed) -> markNoVoted on its way
     * out, so the entry is already at noVote=VOTED. This method exists for the
     * cases where that did NOT happen -- the failure was raised before the branch
     * was ever enlisted, or Acta was not installed for that engine -- where the
     * entry would otherwise sit with no vote and no decision, and the coordinator
     * would wait out its timeout instead of aborting promptly.
     *
     * Deliberately NOT {@link #abortAndVote}: that also calls
     * {@code xa.finishTxn(tid, ABORT)} and votes unconditionally. Here the branch
     * is already rolled back, and voting a second time for a branch that has
     * already voted would be a protocol error -- hence the guard on noVote and
     * phase2 both being unset, evaluated against the durable entry rather than
     * assumed.
     *
     * @return true if this call is what recorded the no vote
     */
    public boolean ensureAbortedAndVoted(String inId) {
        InboxEntry entry = meta.single(c -> ActaMetadata.getInbox(c, inId));
        if (entry == null || entry.noVote != null || entry.phase2 != null) {
            // Already voted (the usual path), already decided, or already collected.
            return false;
        }
        LOGGER.debug("Acta ensureAbortedAndVoted recording a no vote for {} that the XA path did not send", inId);
        markNoVoting(inId);
        twoPc.voteNo(entry.msg.gtid, inId);
        markNoVoted(inId);
        return true;
    }

    /** Lines 31-39. */
    void abortAndVote(Message in, String tid) {
        markNoVoting(in.id);
        xa.finishTxn(tid, Decision.ABORT);
        twoPc.voteNo(in.gtid, in.id);
        markNoVoted(in.id);
    }

    /**
     * Lines 40-51: one contiguous range of sequence numbers per target.
     *
     * Public so a caller driving an activation from OUTSIDE this class (e.g.
     * bench-server's RPC entry point, riding on Seata's own {@code @GlobalTransactional}
     * + ConnectionProxyXA instead of {@link #process}) can assign positions to
     * the outputs it is about to bind via {@link ActaContext#bind}, since
     * {@code ConnectionProxyXA.commit()} calls {@link #recordProgress} directly
     * and never calls this method itself. The caller owns pairing every call
     * with {@link #settlePositions} on every exit path -- see its Javadoc.
     */
    public List<Range> reservePositions(List<Message> msgs) {
        Map<String, List<Message>> groups = new LinkedHashMap<>();
        for (Message m : msgs) {
            groups.computeIfAbsent(m.target, k -> new ArrayList<>()).add(m);
        }
        List<Range> ranges = new ArrayList<>();
        for (Map.Entry<String, List<Message>> e : groups.entrySet()) {
            String target = e.getKey();
            List<Message> group = e.getValue();
            AtomicLong counter = gNextSeq.computeIfAbsent(target, k -> new AtomicLong(0L));
            long begin = counter.getAndAdd(group.size());
            ranges.add(new Range(target, begin, begin + group.size()));
            long seq = begin;
            for (Message m : group) {
                m.pos = new StreamPos(gEpoch, seq);
                seq++;
            }
        }
        return ranges;
    }

    /**
     * Lines 52-58. After this returns, every position below
     * gSettle[target].end is resolved: either an outbox entry exists for it, or
     * no entry can ever appear later. tryGcOutbox relies on that to know its
     * scanned prefix is closed to new insertions.
     *
     * Public counterpart to {@link #reservePositions} for the same reason.
     * MUST run on every exit path after a matching {@link #reservePositions}
     * call, success or failure -- skipping it lets {@code tryGcOutbox} advance
     * a watermark past a position that later gets filled, so the receiver
     * silently drops the message. Belongs in the caller's {@code finally},
     * after {@code ActaContext.unbind()}.
     */
    public void settlePositions(List<Range> ranges) {
        for (Range r : ranges) {
            Settle settle = gSettle.computeIfAbsent(r.target, k -> new Settle());
            synchronized (settle.mutex) {
                settle.ranges.put(r.begin, r.end);
                Long next;
                while ((next = settle.ranges.remove(settle.end)) != null) {
                    settle.end = next;
                }
            }
        }
    }

    long settledEnd(String target) {
        Settle settle = gSettle.computeIfAbsent(target, k -> new Settle());
        synchronized (settle.mutex) {
            return settle.end;
        }
    }

    // --------------------------------------------------- finish (Figure 3)

    /**
     * Apply a phase-2 decision. May run concurrently with process() when the
     * coordinator decides to abort early. Input recovery keys off COMMITTING /
     * ABORTING; inbox GC keys off COMMITTED / ABORTED.
     */
    public void finish(String inId, Decision decision) {
        String tid = markPhase2Start(inId, decision);

        if (tid != null) {
            xa.finishTxn(tid, decision);
        }

        markPhase2Done(inId, tid, decision);
    }

    /**
     * Figure 3, first step: read the recorded tid and, unless the entry is
     * already terminal, record COMMITTING/ABORTING. The COMMITTED/ABORTED
     * guard is what stops a TC redelivering its decision from knocking a
     * finished entry back to COMMITTING. Returns the tid (or null if the
     * entry is gone), so the caller knows whether there is a branch left to
     * finish. Split out so {@code ConnectionProxyXA.xaCommit()}/{@code xaRollback()}
     * can call this BEFORE the real XA commit/rollback -- which IS
     * DB::finishTxn there -- instead of also calling {@link #finish}, which
     * would run finishTxn a second time.
     */
    public String markPhase2Start(String inId, Decision decision) {
        return meta.atomic(c -> {
            InboxEntry e = ActaMetadata.getInbox(c, inId);
            if (e == null) {
                return null;
            }
            if (e.phase2 != Phase2.COMMITTED && e.phase2 != Phase2.ABORTED) {
                ActaMetadata.setPhase2(c, inId, decision == Decision.COMMIT ? Phase2.COMMITTING : Phase2.ABORTING);
            }
            return e.tid;
        });
    }

    /**
     * Figure 3, third step: record COMMITTED/ABORTED after the DB call has
     * returned. {@code tid} is the same value passed to {@link #recordProgress}
     * for this activation (callers already have it -- ConnectionProxyXA as
     * xaXid.toString(), {@link #finish} from {@link #markPhase2Start}'s return
     * -- so this never needs its own metadata lookup to find it); removes it
     * from the pendingTids pre-filter now that no further phase-2 callback is
     * expected for it. {@code tid} may be null (entry already gone by
     * markPhase2Start), in which case there is nothing to remove.
     */
    public void markPhase2Done(String inId, String tid, Decision decision) {
        // In-memory bookkeeping stays synchronous: it costs nothing and keeps
        // the pre-filter tight no matter when the durable write lands.
        forgetTid(inId, tid);
        try {
            phase2DoneExecutor.execute(() -> writePhase2Done(inId, decision));
        } catch (RejectedExecutionException e) {
            // The executor is already shutting down. Write inline rather than
            // drop it, so a graceful shutdown still records what it can.
            writePhase2Done(inId, decision);
        }
    }

    /**
     * The atomic block {@link #markPhase2Done} hands to the executor. Identical
     * to what it used to run inline; only the thread it runs on changed.
     *
     * Failures are logged at WARN and swallowed: there is no caller left to
     * propagate to (the RM callback thread returned long ago), and the entry
     * simply stays at COMMITTING/ABORTING, which recovery and TC redelivery
     * already resolve -- see {@link #phase2DoneExecutor}'s Javadoc.
     */
    private void writePhase2Done(String inId, Decision decision) {
        long startNanos = System.nanoTime();
        try {
            meta.atomic(c -> {
                if (ActaMetadata.getInbox(c, inId) != null) {
                    ActaMetadata.setPhase2(c, inId, decision == Decision.COMMIT ? Phase2.COMMITTED : Phase2.ABORTED);
                }
                return null;
            });
        } catch (RuntimeException e) {
            LOGGER.warn(
                    "Acta markPhase2Done write failed for inId={} decision={} on service={}; entry stays at "
                            + "COMMITTING/ABORTING until TC redelivery re-runs phase 2",
                    inId,
                    decision,
                    serviceId,
                    e);
        } finally {
            ActaTiming.record(
                    decision == Decision.COMMIT
                            ? ActaTiming.Site.XA_COMMIT_MARK_PHASE2_DONE_ASYNC
                            : ActaTiming.Site.XA_ROLLBACK_MARK_PHASE2_DONE_ASYNC,
                    System.nanoTime() - startNanos);
        }
    }

    // ------------------------------------------ deliver / receive (Figure 4)

    /**
     * Sender side. Returns when the output has been delivered or has become
     * unnecessary. Safe to call concurrently for different outputs, never for
     * the same output.
     */
    public void deliver(OutboxEntry msg) {
        ActaTiming.count(ActaTiming.Site.DELIVER_TOTAL);
        long backoffMs = 5L;
        final long deliverStart = System.currentTimeMillis();
        while (true) {
            // Test-only: -1 by default, i.e. this never fires and deliver()
            // retries indefinitely as Acta really does. See
            // ActaFailureInjector.deliverRetryBudgetMs for why the Figure 1c
            // baseline arm removes exactly this and nothing else.
            if (ActaFailureInjector.deliverBudgetExhausted(System.currentTimeMillis() - deliverStart)) {
                throw new IllegalStateException(
                        "deliver retry budget exhausted for " + msg.msg.id + " (test-only baseline arm)");
            }
            InboxEntry in = meta.single(c -> ActaMetadata.getInbox(c, msg.inId));

            // Lines 76-78. Each clause is a distinct reason not to transmit:
            //   in == null       -> the outbox entry (and this one) was GC'd;
            //                       our local copy is stale.
            //   noVote != null   -> we voted no locally, workflow cannot commit.
            //   phase2 ABORTING/ABORTED -> global abort.
            //   phase2 COMMITTING/COMMITTED -> already delivered; only our own
            //                       delivery state is stale after a crash.
            //   tid mismatch     -> output belongs to an earlier, unprepared
            //                       execution that the database rolled back.
            if (in == null || in.noVote != null || in.phase2 != null || !Objects.equals(in.tid, msg.tid)) {
                // Instrumentation only -- diagnosing where deliver()'s time goes,
                // not a fix. DEBUG, not INFO: deliver() runs once per output
                // message, so at benchmark rates these lines are thousands per
                // second and their own I/O cost shows up as "Acta overhead".
                // See CLAUDE.md's Task 6 measurement hygiene notes.
                LOGGER.debug(
                        "deliver bail-out t={} msgId={} tid={} in.tid={} in.phase2={} in.noVote={} in.null={}",
                        System.currentTimeMillis(),
                        msg.msg.id,
                        msg.tid,
                        in == null ? null : in.tid,
                        in == null ? null : in.phase2,
                        in == null ? null : in.noVote,
                        in == null);
                return;
            }

            // Line 79: only transmit once the producing branch is prepared.
            // Timed separately from META_ATOMIC because this is the one Acta
            // query that lands on the APPLICATION database -- see
            // ActaTiming.Site.DELIVER_IS_PREPARED's Javadoc.
            long preparedT0 = System.nanoTime();
            boolean prepared = xa.isPrepared(msg.tid);
            ActaTiming.record(ActaTiming.Site.DELIVER_IS_PREPARED, System.nanoTime() - preparedT0);
            // Instrumentation only (DEBUG -- see the bail-out log above).
            LOGGER.debug(
                    "deliver attempt t={} msgId={} tid={} isPrepared={} backoffMs={}",
                    System.currentTimeMillis(),
                    msg.msg.id,
                    msg.tid,
                    prepared,
                    backoffMs);
            if (prepared) {
                long transportT0 = System.nanoTime();
                try {
                    transport.receive(msg.msg.target, msg.msg);
                    ActaTiming.record(ActaTiming.Site.DELIVER_TRANSPORT_RECEIVE, System.nanoTime() - transportT0);
                } catch (ActaActivationAbortedException aborted) {
                    // The consumer answered, and the answer is "this activation
                    // failed and I have voted no". Nothing was lost, so Figure 5
                    // retransmission does not apply: retrying would re-execute the
                    // consumer under a fresh tid until it happened to succeed and
                    // would hide a real conflict from the client. Mark the entry
                    // terminal and let the abort travel up to whoever owns the
                    // global transaction -- see the exception's Javadoc.
                    ActaTiming.record(ActaTiming.Site.DELIVER_TRANSPORT_RECEIVE, System.nanoTime() - transportT0);
                    ActaTiming.count(ActaTiming.Site.DELIVER_CONSUMER_ABORTED);
                    LOGGER.debug(
                            "deliver consumer aborted t={} msgId={} tid={} reason={}",
                            System.currentTimeMillis(),
                            msg.msg.id,
                            msg.tid,
                            aborted.reason());
                    meta.atomic(c -> {
                        if (ActaMetadata.getOutbox(c, msg.msg.id) != null) {
                            ActaMetadata.setDelivery(c, msg.msg.id, Delivery.FAILED);
                        }
                        return null;
                    });
                    throw aborted;
                } catch (Exception e) {
                    ActaTiming.record(ActaTiming.Site.DELIVER_TRANSPORT_RECEIVE, System.nanoTime() - transportT0);
                    LOGGER.debug(
                            "deliver transport.receive failed t={} msgId={} tid={}: {}",
                            System.currentTimeMillis(),
                            msg.msg.id,
                            msg.tid,
                            e.toString());
                    sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 1000L);
                    continue;
                }
                meta.atomic(c -> {
                    if (ActaMetadata.getOutbox(c, msg.msg.id) != null) {
                        ActaMetadata.setDelivery(c, msg.msg.id, Delivery.ACKED);
                    }
                    return null;
                });
                return;
            }

            sleep(backoffMs);
            backoffMs = Math.min(backoffMs * 2, 1000L);
        }
    }

    /**
     * Target side (lines 85-90). Durably insert or deduplicate. A message at or
     * below the retire watermark is rejected, which is what stops a delayed
     * delivery from recreating a garbage-collected inbox entry.
     */
    public void receive(Message msg) {
        meta.atomic(c -> {
            StreamPos wm = ActaMetadata.getWatermark(c, msg.source);
            if (msg.pos.compareTo(wm) > 0 && ActaMetadata.getInbox(c, msg.id) == null) {
                ActaMetadata.insertInboxIfAbsent(c, msg);
            }
            return null;
        });
    }

    /**
     * Blocking, retrying watermark advance. Callers (tryGcOutbox) must not
     * delete outbox entries until this returns.
     */
    void transportAdvanceWatermark(String target, StreamPos pos) {
        long backoffMs = 5L;
        while (true) {
            try {
                transport.advanceWatermark(target, serviceId, pos);
                return;
            } catch (Exception e) {
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 1000L);
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
