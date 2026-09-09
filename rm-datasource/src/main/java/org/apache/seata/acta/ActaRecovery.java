package org.apache.seata.acta;

import org.apache.seata.acta.ActaTypes.Delivery;
import org.apache.seata.acta.ActaTypes.InboxEntry;
import org.apache.seata.acta.ActaTypes.NoVote;
import org.apache.seata.acta.ActaTypes.OutboxEntry;
import org.apache.seata.acta.ActaTypes.Phase2;
import org.apache.seata.acta.ActaTypes.RecoverAction;
import org.apache.seata.acta.ActaTypes.StreamPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Service startup, input recovery and garbage collection (paper Figures 5, 6).
 *
 * Ordering that matters on restart:
 *   1. wait for DATABASE recovery to finish (unprepared branches rolled back,
 *      prepared ones retained) -- Acta reads that outcome, it does not redo it;
 *   2. startIncarnation();
 *   3. recoverInput() over every durable inbox entry;
 *   4. only then start the inbox dispatcher and outbox poller.
 * Starting the dispatcher earlier would let it process an input whose previous
 * attempt is still prepared, which is exactly the divergent re-execution the
 * strawman in §3 produces.
 */
public final class ActaRecovery {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActaRecovery.class);

    private final ActaService svc;

    public ActaRecovery(ActaService svc) {
        this.svc = svc;
    }

    /**
     * Figure 6 lines 121-126. INFO, not DEBUG: the crash tests assert this ran
     * (and with which epoch) from sonata.log, not just from database state.
     */
    public long startIncarnation() {
        long epoch = svc.meta().atomic(c -> ActaMetadata.incrementEpoch(c, svc.serviceId()));
        svc.resetIncarnation(epoch);
        LOGGER.info("Acta startIncarnation service={} epoch={}", svc.serviceId(), epoch);
        return epoch;
    }

    /**
     * Figure 6 lines 127-143. Returns SKIP when no re-execution is needed, or
     * DISPATCH when it is safe to process the input from scratch.
     *
     * Logs one INFO line per call, always -- the crash tests need to tell
     * "this entry was scanned and correctly judged safe to skip" apart from
     * "recovery never looked at it", and the database state left behind is
     * identical in both cases. Split into one branch per distinct reason
     * (rather than the original single compound condition) purely so each
     * branch can carry its own reason string; the SKIP/DISPATCH outcomes are
     * unchanged.
     */
    public RecoverAction recoverInput(InboxEntry in) {
        // Read the prepare status of the RECORDED tid first, then re-read the
        // current entry. Order matters: the entry may be collected concurrently.
        boolean prepared = in.tid != null && svc.xa().isPrepared(in.tid);

        InboxEntry current = svc.meta().single(c -> ActaMetadata.getInbox(c, in.msg.id));

        if (current == null) {
            // Collected already; normal operation (or a prior recovery pass)
            // already drove this input to a terminal, GC'd state.
            logRecoverInput(in.msg.id, in.tid, prepared, RecoverAction.SKIP, "collected");
            return RecoverAction.SKIP;
        }

        if (current.noVote == NoVote.VOTED) {
            // The coordinator already has our phase-1 no vote.
            logRecoverInput(in.msg.id, in.tid, prepared, RecoverAction.SKIP, "noVoteAlreadyVoted");
            return RecoverAction.SKIP;
        }

        if (current.phase2 != null) {
            // The coordinator already delivered a decision; normal operation
            // (finish()) drives the rest.
            logRecoverInput(in.msg.id, in.tid, prepared, RecoverAction.SKIP, "phase2Set:" + current.phase2);
            return RecoverAction.SKIP;
        }

        if (current.noVote == NoVote.VOTING) {
            // The database rolled the unprepared branch back, but our no vote
            // may not have reached the coordinator. Resend it.
            svc.twoPc().voteNo(current.msg.gtid, current.msg.id);
            svc.meta().atomic(c -> {
                if (ActaMetadata.getInbox(c, current.msg.id) != null) {
                    ActaMetadata.setNoVote(c, current.msg.id, NoVote.VOTED);
                }
                return null;
            });
            logRecoverInput(in.msg.id, in.tid, prepared, RecoverAction.SKIP, "resentNoVote");
            return RecoverAction.SKIP;
        }

        if (prepared) {
            // Effects of the previous attempt survived. Resend the yes vote.
            svc.twoPc().voteYes(in.msg.gtid, in.msg.id);
            logRecoverInput(in.msg.id, in.tid, prepared, RecoverAction.SKIP, "preparedResentYesVote");
            return RecoverAction.SKIP;
        }

        // Nothing effective remains from the previous attempt.
        logRecoverInput(in.msg.id, in.tid, prepared, RecoverAction.DISPATCH, "notPrepared");
        return RecoverAction.DISPATCH;
    }

    private void logRecoverInput(String msgId, String tid, boolean prepared, RecoverAction decision, String reason) {
        LOGGER.info(
                "Acta recoverInput service={} msgId={} tid={} isPrepared={} decision={} reason={}",
                svc.serviceId(),
                msgId,
                tid,
                prepared,
                decision,
                reason);
    }

    /** Upper bound on background retransmission threads per recoverAll() call. */
    private static final int MAX_RETRANSMIT_THREADS = 4;

    /**
     * Full startup sequence. {@code dispatch} hands an input to the inbox
     * dispatcher (which will call ActaService.process).
     *
     * listInbox()/listAllOutbox() are unfiltered by design -- ActaMetadata has
     * no notion of "this service's rows" -- because a deployment can share one
     * metadata store across several ActaService instances (this integration's
     * single shared acta_meta backing both a mysql-branch and a pg-branch
     * service, per CLAUDE.md). Filtering here, by target for inbox and by
     * source for outbox, is required correctness: recoverInput()/deliver()
     * both call svc.xa().isPrepared(tid) using THIS service's XaOps, and an
     * entry belonging to the OTHER engine's tid format would be silently
     * misjudged as "not prepared" against the wrong database.
     *
     * No special recovery is needed for outputs beyond retransmission: an
     * output still NEW after a restart repeats the normal validation inside
     * deliver() (tid match, phase2 state), an ACKED one needs nothing. Unlike
     * recoverInput(), which CLAUDE.md requires to finish before new work is
     * accepted, retransmission is not part of that ordering guarantee, so it
     * runs on a background pool instead of blocking the caller -- deliver()
     * blocks/retries indefinitely until its target is reachable or the entry
     * becomes moot, and a target being down at startup must not hold up
     * everything else. This is race-free against a live request's own
     * deliver() calls: every output produced after this method returns gets a
     * FRESH message id (never a pre-existing one from this scan), so the two
     * never touch the same outbox row.
     *
     * Logs (INFO): one summary line up front with the scanned/considered
     * counts, one line per owned outbox entry (delivery state and whether it
     * was queued for retransmission), a closing summary with the dispatched
     * and retransmit-queued counts, and -- via recoverInput/startIncarnation
     * -- one line per inbox entry and one for the epoch bump. Together these
     * are what let a crash test distinguish "recovery ran and correctly did
     * nothing" from "recovery never ran": the database ends up identical
     * either way.
     */
    public void recoverAll(Consumer<InboxEntry> dispatch) {
        long epoch = startIncarnation();

        // NO source/target filtering here, deliberately. It used to filter
        // `e.msg.target.equals(svc.serviceId())` because ONE shared acta_meta
        // backed both services, so listInbox returned the other engine's rows
        // too and isPrepared would judge them with the wrong tid format.
        //
        // With per-service stores (paper §3.2) every row in THIS store is by
        // construction this service's own, so the filter is not merely
        // redundant -- keeping it would be harmful. It would silently swallow
        // a mis-routed store: if this service were ever handed the wrong
        // ActaMetadata, the filter would quietly drop every entry and
        // recoverAll would report inboxScanned=0 and look like a clean
        // recovery. Without it, a mis-routing shows up as entries recovered
        // under the wrong engine, which is loud. The inboxTotal/inboxScanned
        // pair below is kept and now always equal -- a divergence between them
        // is itself the signal that a store is shared or mis-wired.
        List<InboxEntry> owned = svc.meta().single(ActaMetadata::listInbox);
        List<OutboxEntry> ownedOutputs = svc.meta().single(ActaMetadata::listAllOutbox);

        LOGGER.info(
                "Acta recoverAll service={} epoch={} inboxScanned={} inboxTotal={} outboxConsidered={} outboxTotal={}",
                svc.serviceId(),
                epoch,
                owned.size(),
                owned.size(),
                ownedOutputs.size(),
                ownedOutputs.size());

        int dispatched = 0;
        for (InboxEntry e : owned) {
            if (recoverInput(e) == RecoverAction.DISPATCH) {
                dispatched++;
                dispatch.accept(e);
            }
        }

        List<OutboxEntry> retransmit = new ArrayList<>();
        for (OutboxEntry o : ownedOutputs) {
            boolean queued = o.delivery == Delivery.NEW;
            LOGGER.info(
                    "Acta recoverAll outbox service={} msgId={} inId={} delivery={} deliverQueued={}",
                    svc.serviceId(),
                    o.msg.id,
                    o.inId,
                    o.delivery,
                    queued);
            if (queued) {
                retransmit.add(o);
            }
        }

        LOGGER.info(
                "Acta recoverAll service={} epoch={} dispatched={} retransmitQueued={}",
                svc.serviceId(),
                epoch,
                dispatched,
                retransmit.size());

        if (!retransmit.isEmpty()) {
            int threads = Math.min(retransmit.size(), MAX_RETRANSMIT_THREADS);
            ExecutorService pool = new ThreadPoolExecutor(
                    threads, threads, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(retransmit.size()), r -> {
                        Thread t = new Thread(r, "acta-retransmit-" + svc.serviceId());
                        t.setDaemon(true);
                        return t;
                    });
            for (OutboxEntry o : retransmit) {
                pool.submit(() -> svc.deliver(o));
            }
            // Accepted tasks keep running to completion after shutdown(); this
            // only stops the pool from taking NEW submissions, which recoverAll()
            // never makes again after this point anyway.
            pool.shutdown();
        }
    }

    // ----------------------------------------------- garbage collection (Fig 5)

    /** Lines 91-96. Requires neither a database query nor a coordinator RPC. */
    public void tryGcInbox(InboxEntry in) {
        svc.meta().atomic(c -> {
            InboxEntry cur = ActaMetadata.getInbox(c, in.msg.id);
            if (cur == null) {
                return null;
            }
            StreamPos wm = ActaMetadata.getWatermark(c, cur.msg.source);
            boolean retired = cur.msg.pos.compareTo(wm) <= 0;
            boolean settled =
                    cur.noVote == NoVote.VOTED || cur.phase2 == Phase2.COMMITTED || cur.phase2 == Phase2.ABORTED;
            boolean noOutputs = !ActaMetadata.hasOutputsFrom(c, cur.msg.id);
            if (retired && settled && noOutputs) {
                ActaMetadata.deleteInbox(c, cur.msg.id);
                // The row is gone, so getInboxByTid can no longer find it either;
                // drop the in-memory mapping with it so the cache and the
                // database keep answering the same question the same way. Also
                // collects the abortAndVote case (noVote VOTED, phase2 still
                // null), which never reaches markPhase2Done.
                svc.forgetTid(cur.msg.id, cur.tid);
            }
            return null;
        });
    }

    /**
     * Lines 97-116. Scan one target's retained outputs in position order and
     * stop at the first unsettled position or the first NEW output that may
     * still need transmitting.
     *
     * Stopping at the first UNSETTLED position is the subtle part: it keeps the
     * scanned prefix closed to later insertions. Without it, advancing the
     * target's watermark could make an output inserted afterwards be silently
     * discarded by the target's receive().
     */
    public void tryGcOutbox(String target) {
        final long settledEnd = svc.settledEnd(target);
        final long epoch = svc.epoch();

        StreamPos[] endHolder = new StreamPos[1];
        List<String> gcIds = new ArrayList<>();

        svc.meta().atomic(c -> {
            List<OutboxEntry> sorted = ActaMetadata.listOutboxByTarget(c, target);
            for (OutboxEntry msg : sorted) {
                if (msg.msg.pos.epoch == epoch && msg.msg.pos.seq >= settledEnd) {
                    break;
                }
                if (msg.delivery == Delivery.NEW) {
                    InboxEntry in = ActaMetadata.getInbox(c, msg.inId);
                    if (in != null && in.noVote == null && in.phase2 == null && Objects.equals(in.tid, msg.tid)) {
                        break; // transmission may still be required
                    }
                }
                endHolder[0] = msg.msg.pos;
                gcIds.add(msg.msg.id);
            }
            return null;
        });

        if (endHolder[0] == null) {
            return;
        }

        // Advance the TARGET's watermark first, remove locally only after the
        // RPC returns (line 114).
        svc.transportAdvanceWatermark(target, endHolder[0]);

        svc.meta().atomic(c -> {
            for (String id : gcIds) {
                ActaMetadata.deleteOutbox(c, id);
            }
            return null;
        });
    }

    /** Receiver side of advanceWatermark (lines 117-120). */
    public static void advanceWatermark(ActaMetadata meta, String source, StreamPos pos) {
        meta.atomic(c -> {
            ActaMetadata.advanceWatermark(c, source, pos);
            return null;
        });
    }

    /** Convenience for tests that drive GC synchronously. */
    public void gcSweep(List<String> targets) {
        for (String t : targets) {
            tryGcOutbox(t);
        }
        List<InboxEntry> entries = svc.meta().single(ActaMetadata::listInbox);
        for (InboxEntry e : entries) {
            tryGcInbox(e);
        }
    }
}
