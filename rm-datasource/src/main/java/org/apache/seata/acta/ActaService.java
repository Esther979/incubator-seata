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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
     * In-memory pre-filter for tids Acta has recorded progress for, so
     * ConnectionProxyXA.xaCommit()/xaRollback() -- which run on Seata's RM
     * callback thread for EVERY branch in the process, Acta-driven or not --
     * can skip resolveInId's acta_meta round trip for the common case of a
     * branch Acta was never told about. A concurrent set: xaCommit/xaRollback
     * run on RM threads while recordProgress runs on request threads.
     *
     * Keyed on exactly the string resolveInId takes (the branch's
     * XAXid.toString()); recordProgress adds the identical value it is
     * called with, which ConnectionProxyXA.commit() sources from the same
     * xaBranchXid.toString(). A mismatched key format here would make every
     * lookup miss and silently disable phase-2 tracking, not just the
     * optimization.
     */
    private final Set<String> pendingTids = ConcurrentHashMap.newKeySet();

    public ActaService(
            String serviceId, ActaMetadata meta, XaOps xa, TwoPcClient twoPc, Handler handler, Transport transport) {
        this.serviceId = serviceId;
        this.meta = meta;
        this.xa = xa;
        this.twoPc = twoPc;
        this.handler = handler;
        this.transport = transport;
        primePendingTids();
    }

    /**
     * Figure 6 prerequisite for the pre-filter above: after a crash, prepared
     * branches exist that the TC will still drive xaCommit/xaRollback for, but
     * pendingTids starts empty on a fresh instance. Without this, a recovering
     * branch's phase-2 callback would look like "not Acta's", skip
     * markPhase2Done, and leave phase2 NULL forever -- tryGcInbox never
     * collects that entry. Runs from the constructor so there is no window
     * between construction and use where the set could be consulted unprimed.
     */
    private void primePendingTids() {
        List<InboxEntry> entries = meta.atomic(ActaMetadata::listInbox);
        for (InboxEntry e : entries) {
            if (e.tid != null && e.phase2 != Phase2.COMMITTED && e.phase2 != Phase2.ABORTED) {
                pendingTids.add(e.tid);
            }
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
            // its eventual phase-2 callback through to resolveInId.
            pendingTids.add(tid);
        }
        return ok;
    }

    /**
     * Cheap in-memory check for ConnectionProxyXA.xaCommit()/xaRollback(): is
     * this tid one Acta has recorded progress for? A hash lookup, no
     * acta_meta round trip -- see pendingTids' Javadoc for why this exists.
     */
    public boolean isPending(String tid) {
        return pendingTids.contains(tid);
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
        InboxEntry e = meta.atomic(c -> ActaMetadata.getInboxByTid(c, tid));
        return e == null ? null : e.msg.id;
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
        meta.atomic(c -> {
            if (ActaMetadata.getInbox(c, inId) != null) {
                ActaMetadata.setPhase2(c, inId, decision == Decision.COMMIT ? Phase2.COMMITTED : Phase2.ABORTED);
            }
            return null;
        });
        if (tid != null) {
            pendingTids.remove(tid);
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
            InboxEntry in = meta.atomic(c -> ActaMetadata.getInbox(c, msg.inId));

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
