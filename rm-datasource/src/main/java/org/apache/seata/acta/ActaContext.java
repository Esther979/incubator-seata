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

    private static final ThreadLocal<Binding> HOLDER = new ThreadLocal<>();

    private ActaContext() {}

    public static void bind(String inId, List<Message> outputs) {
        HOLDER.set(new Binding(inId, outputs == null ? Collections.emptyList() : outputs));
    }

    public static void unbind() {
        HOLDER.remove();
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
