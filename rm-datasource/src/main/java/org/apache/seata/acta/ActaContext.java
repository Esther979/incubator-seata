package org.apache.seata.acta;

import org.apache.seata.acta.ActaTypes.Message;

import java.util.Collections;
import java.util.List;

/**
 * Per-request carrier for the current Acta input message id and the outputs
 * produced by {@link ActaSpi.Handler#execute}, mirroring how
 * {@code org.apache.seata.core.context.RootContext} carries the xid.
 *
 * The RPC entry point must call {@link #bind} after running the handler and
 * before the connection's autocommit-driven {@code commit()} fires, and
 * {@link #unbind} in a finally so the ThreadLocal never leaks across requests
 * on a pooled thread.
 */
public final class ActaContext {

    private static final class Binding {
        final String inId;
        final List<Message> outputs;

        Binding(String inId, List<Message> outputs) {
            this.inId = inId;
            this.outputs = outputs;
        }
    }

    /**
     * A phase-1 failure that the application thread would otherwise never see.
     *
     * <h2>Why this exists</h2>
     *
     * ConnectionProxyXA.close() is where the XA branch is prepared, and Spring
     * calls it from DataSourceUtils.releaseConnection during
     * doCleanupAfterCompletion -- which logs and SWALLOWS any SQLException it
     * throws. So a failure raised at PREPARE (a PostgreSQL SSI pivot detected at
     * PREPARE TRANSACTION, an ActaProgressException, any PGXAException) never
     * reaches the caller: the @Transactional method returns normally, the
     * activation looks successful, the receiver answers 200, and the root
     * commits -- while the branch has already reported PhaseOne_Failed and will
     * never be committed. Observed once in ~200 concurrent workflows as a root
     * that committed over a leaf that had voted no.
     *
     * The branch-level handling is correct and unchanged; only its VISIBILITY
     * was missing. close() records the failure here on the same thread that ran
     * the activation, right where it reports PhaseOne_Failed, and the activation
     * runner checks for it after the body returns. Nothing about the XA hook
     * order changes -- this is a side channel for a failure that has already
     * been decided, not a new decision point.
     */
    public static final class Phase1Failure {
        private final String tid;
        private final String reason;
        private final Throwable cause;

        Phase1Failure(String tid, Throwable cause) {
            this.tid = tid;
            this.cause = cause;
            this.reason = rootReason(cause);
        }

        /** The failing XA branch's tid. */
        public String tid() {
            return tid;
        }

        /** Simple class name of the root cause, for the 409 body's "reason". */
        public String reason() {
            return reason;
        }

        public Throwable cause() {
            return cause;
        }

        /**
         * The deepest cause's class name. A prepare failure arrives wrapped
         * (SQLException around PGXAException, say), and the wrapper names the
         * plumbing rather than the conflict -- which is the whole point of
         * reporting a reason at all.
         */
        private static String rootReason(Throwable cause) {
            if (cause == null) {
                return "UnknownPhase1Failure";
            }
            Throwable root = cause;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            return root.getClass().getSimpleName();
        }
    }

    private static final ThreadLocal<Binding> HOLDER = new ThreadLocal<>();
    private static final ThreadLocal<Phase1Failure> PHASE1_FAILURE = new ThreadLocal<>();

    private ActaContext() {}

    public static void bind(String inId, List<Message> outputs) {
        HOLDER.set(new Binding(inId, outputs == null ? Collections.emptyList() : outputs));
        // A fresh activation starts with no recorded failure. Without this, a
        // phase-1 failure left behind by an earlier, UNBOUND use of this pooled
        // thread -- a rollback outside any activation, say -- would be taken by
        // this activation and abort it for something that never happened to it.
        // unbind() alone is not enough: it only runs for threads that were bound.
        PHASE1_FAILURE.remove();
    }

    public static void unbind() {
        HOLDER.remove();
        // Cleared with the binding so a failure recorded by a path that never
        // checks it cannot leak onto the next request on this pooled thread.
        PHASE1_FAILURE.remove();
    }

    /**
     * Record that this thread's XA branch failed in phase 1. Called by
     * ConnectionProxyXA where it reports PhaseOne_Failed -- see
     * {@link Phase1Failure}. Last write wins: a branch that fails twice is still
     * one failed activation, and the most recent cause is the informative one.
     */
    public static void recordPhase1Failure(String tid, Throwable cause) {
        PHASE1_FAILURE.set(new Phase1Failure(tid, cause));
    }

    /**
     * Consume any recorded phase-1 failure, clearing it. Get-and-clear rather
     * than a plain getter so a caller cannot check it twice and abort two
     * activations for one failure.
     */
    public static Phase1Failure takePhase1Failure() {
        Phase1Failure failure = PHASE1_FAILURE.get();
        if (failure != null) {
            PHASE1_FAILURE.remove();
        }
        return failure;
    }

    /** Drop any recorded phase-1 failure without acting on it. */
    public static void clearPhase1Failure() {
        PHASE1_FAILURE.remove();
    }

    /** The current input message id, or null if this thread has no Acta activation bound. */
    public static String getInId() {
        Binding b = HOLDER.get();
        return b == null ? null : b.inId;
    }

    public static List<Message> getOutputs() {
        Binding b = HOLDER.get();
        return b == null ? Collections.emptyList() : b.outputs;
    }
}
