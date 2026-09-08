package org.apache.seata.acta;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static handle to the per-engine {@link ActaService}s, so classes in
 * {@code org.apache.seata.rm.datasource.xa} (which cannot be constructor-injected --
 * they are instantiated by Seata's own DataSource/XAConnection plumbing) can
 * reach Acta. Mirrors the codebase's existing static-singleton pattern
 * (e.g. {@code ConfigurationFactory.getInstance()}, {@code DefaultResourceManager.get()}).
 *
 * Installed once at startup by bench-server, once per engine.
 *
 * <h2>Why this is keyed by dbType</h2>
 *
 * It used to hold ONE ActaService for the whole process. That forced both
 * services to share a single metadata store, because ConnectionProxyXA's hook
 * had no way to say which engine's branch it was running for -- the compromise
 * recorded in CLAUDE.md's Task 3 notes.
 *
 * Paper §3.2 has each service storing its Inbox/Outbox/Epoch in ITS OWN local
 * database, and the shared store turned out to cost real throughput: at
 * concurrency 4, 68% of every acta_meta transaction's latency was queueing on
 * the one shared commit path (2.62 ms contention-free floor against 8.13 ms
 * measured), worth ~111 ms per workflow. See
 * results/2026-09-03-acta-micro-baseline.md.
 *
 * The blocker was never structural: every call site is an instance method of
 * ConnectionProxyXA with {@code resource.getDbType()} in scope, and four of
 * them already used exactly that expression to pick a service id for
 * ActaFailureInjector. Keying this map is what turns that latent capability
 * into the per-service stores the paper specifies.
 *
 * Keys are normalized lowercase dbType strings ({@code "mysql"},
 * {@code "postgresql"}), matching the case-insensitive comparisons
 * ConnectionProxyXA already performs against {@code DBType.*.name()}.
 */
public final class ActaRuntime {

    private static final Map<String, ActaService> INSTANCES = new ConcurrentHashMap<>();

    private ActaRuntime() {}

    private static String key(String dbType) {
        return dbType == null ? "" : dbType.toLowerCase(Locale.ROOT);
    }

    public static void install(String dbType, ActaService svc) {
        INSTANCES.put(key(dbType), svc);
    }

    /**
     * True once {@link #install} has run FOR THIS ENGINE. "Not installed" is the
     * NORMAL state whenever Acta is disabled (a baseline run, a branch not
     * driven by Acta's message layer, or simply an engine Acta was never wired
     * for) -- every hook site in ConnectionProxyXA must check this BEFORE
     * calling {@link #get}, not treat it as a startup-ordering bug.
     */
    public static boolean isInstalled(String dbType) {
        return INSTANCES.containsKey(key(dbType));
    }

    /**
     * The ActaService owning THIS engine's metadata store. Routing by engine is
     * correctness-critical, not a convenience: with per-service stores, asking
     * the wrong one about a tid finds no row at all, and recoverInput would
     * read that as "never recorded progress" and re-execute an activation whose
     * previous attempt may still be prepared.
     */
    public static ActaService get(String dbType) {
        ActaService svc = INSTANCES.get(key(dbType));
        if (svc == null) {
            throw new IllegalStateException("ActaRuntime.install() was never called for dbType '" + dbType + "'");
        }
        return svc;
    }
}
