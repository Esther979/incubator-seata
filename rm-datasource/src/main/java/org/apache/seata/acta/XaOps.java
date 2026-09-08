package org.apache.seata.acta;

import org.apache.seata.acta.ActaTypes.Decision;
import org.apache.seata.rm.datasource.xa.XAXid;
import org.apache.seata.rm.datasource.xa.XAXidBuilder;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The four database operations Acta needs (DB::beginTxn / prepareTxn /
 * finishTxn / isPrepared).
 *
 * isPrepared is the whole point of the design: it is how a recovering service
 * asks the database "did the effects of my previous attempt survive?" without
 * having logged anything per data access. Sonata already needs the same query
 * to clean up dangling helper transactions (Sonata §3.5), so the plumbing is
 * likely already present in the artifact.
 */
public interface XaOps {

    /** Start an XA branch under the given tid and return its connection. */
    Connection beginTxn(String tid) throws SQLException;

    /** XA END + XA PREPARE. Returns false if the database refused to prepare. */
    boolean prepareTxn(String tid);

    /** XA COMMIT / XA ROLLBACK. Tolerates an already-finished branch. */
    void finishTxn(String tid, Decision decision);

    /** True iff a branch with this tid is currently in the prepared state. */
    boolean isPrepared(String tid);

    // ------------------------------------------------------------------ MySQL

    final class MySql implements XaOps {
        private final DataSource appDs;
        private final ConcurrentHashMap<String, Connection> live = new ConcurrentHashMap<>();

        public MySql(DataSource appDs) {
            this.appDs = appDs;
        }

        @Override
        public Connection beginTxn(String tid) throws SQLException {
            Connection c = appDs.getConnection();
            c.setAutoCommit(true); // XA START drives the transaction, not JDBC
            try (Statement s = c.createStatement()) {
                s.execute("XA START '" + esc(tid) + "'");
            }
            live.put(tid, c);
            return c;
        }

        @Override
        public boolean prepareTxn(String tid) {
            Connection c = live.get(tid);
            if (c == null) {
                return false;
            }
            try (Statement s = c.createStatement()) {
                s.execute("XA END '" + esc(tid) + "'");
                s.execute("XA PREPARE '" + esc(tid) + "'");
                return true;
            } catch (SQLException e) {
                return false;
            } finally {
                closeQuietly(live.remove(tid));
            }
        }

        @Override
        public void finishTxn(String tid, Decision decision) {
            Connection open = live.remove(tid);
            if (open != null) {
                // Not yet prepared: end the branch before rolling it back.
                try (Statement s = open.createStatement()) {
                    s.execute("XA END '" + esc(tid) + "'");
                } catch (SQLException ignored) {
                }
                try (Statement s = open.createStatement()) {
                    s.execute("XA ROLLBACK '" + esc(tid) + "'");
                } catch (SQLException ignored) {
                }
                closeQuietly(open);
                return;
            }
            String sql =
                    decision == Decision.COMMIT ? "XA COMMIT '" + esc(tid) + "'" : "XA ROLLBACK '" + esc(tid) + "'";
            try (Connection c = appDs.getConnection();
                    Statement s = c.createStatement()) {
                s.execute(sql);
            } catch (SQLException ignored) {
                // Already finished by a previous attempt; finish() is idempotent.
            }
        }

        @Override
        public boolean isPrepared(String tid) {
            try (Connection c = appDs.getConnection();
                    Statement s = c.createStatement();
                    ResultSet rs = s.executeQuery("XA RECOVER CONVERT XID")) {
                while (rs.next()) {
                    // Match the WHOLE xid (gtrid+bqual), not just the gtrid prefix.
                    // Seata's XAXidBuilder.build(xid, branchId) puts the global xid in
                    // gtrid and the branch id in bqual, so two branches of the same
                    // global transaction share a gtrid and differ only in bqual.
                    // Truncating to gtrid_length bytes before comparing would make
                    // those branches indistinguishable: isPrepared could return true
                    // for a branch that was rolled back, as long as a DIFFERENT branch
                    // of the same global transaction is still prepared.
                    byte[] data = decodeXidData(rs.getString("data"));
                    if (tid.equals(new String(data, StandardCharsets.UTF_8))) {
                        return true;
                    }
                }
                return false;
            } catch (SQLException e) {
                throw new RuntimeException("XA RECOVER failed", e);
            }
        }

        /**
         * MySQL Connector/J (verified on 8.4.0 against MySQL 8.0.39) returns the
         * VARBINARY "data" column of XA RECOVER CONVERT XID as the literal text
         * "0x<hex>", the same rendering the mysql CLI uses for binary columns —
         * NOT the raw xid bytes decoded to a Java String. getString() never
         * equals the tid verbatim; it must be un-hex-encoded first. Confirmed
         * with scripts/verify_xa.sh and a standalone JDBC probe on 2026-09-01.
         */
        private static byte[] decodeXidData(String rendered) {
            if (rendered == null) {
                return new byte[0];
            }
            if (rendered.length() >= 2
                    && rendered.charAt(0) == '0'
                    && (rendered.charAt(1) == 'x' || rendered.charAt(1) == 'X')) {
                int n = (rendered.length() - 2) / 2;
                byte[] bytes = new byte[n];
                for (int i = 0; i < n; i++) {
                    int hi = Character.digit(rendered.charAt(2 + i * 2), 16);
                    int lo = Character.digit(rendered.charAt(2 + i * 2 + 1), 16);
                    bytes[i] = (byte) ((hi << 4) + lo);
                }
                return bytes;
            }
            return rendered.getBytes(StandardCharsets.UTF_8);
        }

        private static String esc(String tid) {
            return tid.replace("'", "''");
        }

        private static void closeQuietly(Connection c) {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    // ------------------------------------------------------------- PostgreSQL

    final class Pg implements XaOps {
        private final DataSource appDs;
        private final ConcurrentHashMap<String, Connection> live = new ConcurrentHashMap<>();

        public Pg(DataSource appDs) {
            this.appDs = appDs;
        }

        @Override
        public Connection beginTxn(String tid) throws SQLException {
            Connection c = appDs.getConnection();
            c.setAutoCommit(false);
            live.put(tid, c);
            return c;
        }

        @Override
        public boolean prepareTxn(String tid) {
            Connection c = live.get(tid);
            if (c == null) {
                return false;
            }
            try (Statement s = c.createStatement()) {
                s.execute("PREPARE TRANSACTION '" + esc(tid) + "'");
                return true;
            } catch (SQLException e) {
                return false;
            } finally {
                closeQuietly(live.remove(tid));
            }
        }

        @Override
        public void finishTxn(String tid, Decision decision) {
            Connection open = live.remove(tid);
            if (open != null) {
                try {
                    open.rollback();
                } catch (SQLException ignored) {
                }
                closeQuietly(open);
                return;
            }
            String sql = decision == Decision.COMMIT
                    ? "COMMIT PREPARED '" + esc(tid) + "'"
                    : "ROLLBACK PREPARED '" + esc(tid) + "'";
            try (Connection c = appDs.getConnection();
                    Statement s = c.createStatement()) {
                s.execute(sql);
            } catch (SQLException ignored) {
            }
        }

        /**
         * A branch reaches pg_prepared_xacts under one of TWO different gids,
         * depending on which path prepared it, and this must match either:
         *
         *  - {@link #prepareTxn} above (the standalone, raw-SQL path used by
         *    {@link ActaService#process}) issues {@code PREPARE TRANSACTION '<tid>'},
         *    so the gid IS the raw tid;
         *  - the real Acta integration prepares through
         *    {@code ConnectionProxyXA} -> PgJDBC's XAResource, which does NOT
         *    use the tid string at all -- see {@link #pgJdbcGid}.
         *
         * Getting this wrong is silent and one-directional: isPrepared returns
         * false for a genuinely prepared branch, so recoverInput reads
         * "nothing survived" and re-executes an activation whose previous
         * attempt is still holding locks. That was the live bug found
         * 2026-09-02 (CLAUDE.md), and the exact mirror of MySQL's Bug 1.
         *
         * Both candidates are matched by FULL equality, never by prefix.
         * A prefix match on the gtrid half would reproduce MySQL's Bug 2,
         * where two branches of one global transaction became
         * indistinguishable and a rolled-back branch could read as prepared.
         */
        @Override
        public boolean isPrepared(String tid) {
            String jdbcGid = pgJdbcGid(tid);
            String sql = jdbcGid == null
                    ? "SELECT 1 FROM pg_prepared_xacts WHERE gid = ?"
                    : "SELECT 1 FROM pg_prepared_xacts WHERE gid = ? OR gid = ?";
            try (Connection c = appDs.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, tid);
                if (jdbcGid != null) {
                    ps.setString(2, jdbcGid);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            } catch (SQLException e) {
                throw new RuntimeException("pg_prepared_xacts lookup failed", e);
            }
        }

        /**
         * The gid PgJDBC's XAResource would prepare {@code tid}'s branch under,
         * or null when {@code tid} is not a Seata XAXid at all (the standalone
         * path's arbitrary tid strings).
         *
         * Format, verified by disassembling
         * {@code org.postgresql.xa.RecoveredXid.xidToString} in
         * postgresql-42.7.3.jar on 2026-09-02 (the method is package-private,
         * so it cannot simply be called):
         *
         * <pre>formatId + '_' + Base64(gtrid) + '_' + Base64(bqual)</pre>
         *
         * with the plain padded {@code java.util.Base64.getEncoder()}. Cross-checked
         * against a real gid captured from postgres' own statement log.
         *
         * The (gtrid, bqual) split is delegated to {@link XAXidBuilder}, i.e. to
         * Seata's OWN encoding, rather than re-deriving it here -- {@code XABranchXid}
         * puts the global xid in gtrid and {@code "-" + branchId} in bqual, and
         * duplicating that rule is how it silently drifts. {@code XABranchXid.toString()}
         * is {@code xid + "-" + branchId}, so the split is at the LAST '-'.
         *
         * A tid that merely happens to end in {@code -<digits>} (e.g. a raw-path
         * uuid) yields a syntactically valid but meaningless gid; that is
         * harmless, because it is then matched by equality against a table that
         * cannot contain it, and the raw-tid candidate still matches on its own.
         */
        static String pgJdbcGid(String tid) {
            int sep = tid.lastIndexOf('-');
            if (sep <= 0 || sep == tid.length() - 1) {
                return null;
            }
            long branchId;
            try {
                branchId = Long.parseLong(tid.substring(sep + 1));
            } catch (NumberFormatException e) {
                return null;
            }
            XAXid xaXid = XAXidBuilder.build(tid.substring(0, sep), branchId);
            Base64.Encoder enc = Base64.getEncoder();
            return xaXid.getFormatId()
                    + "_" + enc.encodeToString(xaXid.getGlobalTransactionId())
                    + "_" + enc.encodeToString(xaXid.getBranchQualifier());
        }

        private static String esc(String tid) {
            return tid.replace("'", "''");
        }

        private static void closeQuietly(Connection c) {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }
}
