package org.apache.seata.acta;

import org.apache.seata.acta.ActaTypes.Delivery;
import org.apache.seata.acta.ActaTypes.InboxEntry;
import org.apache.seata.acta.ActaTypes.Message;
import org.apache.seata.acta.ActaTypes.NoVote;
import org.apache.seata.acta.ActaTypes.OutboxEntry;
import org.apache.seata.acta.ActaTypes.Phase2;
import org.apache.seata.acta.ActaTypes.StreamPos;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Durable metadata store (paper §3.2).
 *
 * The DataSource handed to this class MUST be separate from the application's
 * XA DataSource. Individual reads and writes here are atomic and durable on
 * their own; {@link #atomic} groups several of them into one local transaction,
 * which is the paper's "atomic block".
 */
public final class ActaMetadata {

    /** Body of an atomic block. */
    public interface TxBody<T> {
        T run(Connection c) throws SQLException;
    }

    private final DataSource metaDs;
    private final String serviceId;

    public ActaMetadata(DataSource metaDs, String serviceId) {
        this.metaDs = metaDs;
        this.serviceId = serviceId;
    }

    public String serviceId() {
        return serviceId;
    }

    /**
     * Run several metadata operations as one local transaction.
     *
     * Timed as ActaTiming.Site.META_ATOMIC (measurement only, see its Javadoc):
     * this is the single choke point every acta_meta round trip goes through,
     * so instrumenting here -- rather than at each of the ~20 call sites --
     * gives the per-hop metadata cost with no risk of missing one. The span
     * covers connection checkout too, deliberately: the metadata pool is
     * separately sized (invariant 1) and starving it is a plausible failure
     * mode that a body-only timer would hide.
     */
    public <T> T atomic(TxBody<T> body) {
        long startNanos = System.nanoTime();
        try {
            return atomic0(body);
        } finally {
            ActaTiming.record(ActaTiming.Site.META_ATOMIC, System.nanoTime() - startNanos);
        }
    }

    /**
     * Run ONE statement, with no explicit transaction around it.
     *
     * A lone statement is atomic and durable on its own -- see this class's
     * Javadoc -- so wrapping it in BEGIN/COMMIT buys nothing and costs round
     * trips. Only the round trips are removed; the statement and its durability
     * are unchanged.
     *
     * Counted as META_ATOMIC exactly like {@link #atomic}, so that site keeps
     * meaning "one acta_meta round trip" and count/ACTIVATION_TOTAL remains the
     * measured round-trips-per-hop figure its Javadoc describes.
     *
     * Use ONLY for a body that executes exactly one statement. A body that
     * reads and then writes MUST use {@link #atomic}: without the transaction
     * those two statements commit separately and another connection can
     * interleave between them, which would break exactly the read-modify-write
     * atomicity every phase2 / noVote / watermark guard in this package relies
     * on.
     */
    public <T> T single(TxBody<T> body) {
        long startNanos = System.nanoTime();
        try {
            return single0(body);
        } finally {
            ActaTiming.record(ActaTiming.Site.META_ATOMIC, System.nanoTime() - startNanos);
        }
    }

    private <T> T single0(TxBody<T> body) {
        // No setAutoCommit call at all. Hikari hands the connection over in the
        // pool's configured autoCommit state -- the default true, not overridden
        // for either acta_meta DataSource -- so the statement self-commits.
        try (Connection c = metaDs.getConnection()) {
            return body.run(c);
        } catch (SQLException e) {
            throw new RuntimeException("acta metadata failure", e);
        }
    }

    private <T> T atomic0(TxBody<T> body) {
        try (Connection c = metaDs.getConnection()) {
            c.setAutoCommit(false);
            try {
                T r = body.run(c);
                c.commit();
                return r;
            } catch (RuntimeException | SQLException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                }
                throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
            }
            // No setAutoCommit(old) restore on purpose. HikariCP already does it
            // when the connection goes back to the pool: PoolBase.resetConnectionState
            // (verified against HikariCP 7.0.2, the version on this classpath)
            // checks the connection's dirty bits and, for autoCommit, compares
            // ProxyConnection.getAutoCommitState() with the pool's own
            // isAutoCommit -- HikariConfig.isAutoCommit(), default true and not
            // overridden for either acta_meta DataSource -- calling
            // Connection.setAutoCommit(isAutoCommit) only when the two differ.
            // Restoring it here as well just performed that same reset twice per
            // block, and on MySQL setAutoCommit is a wire round trip
            // (SET autocommit=1), not a local flag. Dropping it removes one round
            // trip per block and cannot leak a dirty flag to the next borrower.
        } catch (SQLException e) {
            throw new RuntimeException("acta metadata failure", e);
        }
    }

    // ---------------------------------------------------------------- inbox

    public static InboxEntry getInbox(Connection c, String msgId) throws SQLException {
        String q = "SELECT msg_id, gtid, source, target, pos_epoch, pos_seq, body, "
                + "tid, no_vote, phase2 FROM acta_inbox WHERE msg_id = ?";
        try (PreparedStatement ps = c.prepareStatement(q)) {
            ps.setString(1, msgId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return readInbox(rs);
            }
        }
    }

    /**
     * Reverse lookup used by {@code ConnectionProxyXA.xaCommit()}/{@code xaRollback()},
     * which run on the RM's phase-2 callback thread and know only the tid
     * (derived from the branch's XAXid), never the input message id.
     */
    public static InboxEntry getInboxByTid(Connection c, String tid) throws SQLException {
        String q = "SELECT msg_id, gtid, source, target, pos_epoch, pos_seq, body, "
                + "tid, no_vote, phase2 FROM acta_inbox WHERE tid = ?";
        try (PreparedStatement ps = c.prepareStatement(q)) {
            ps.setString(1, tid);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return readInbox(rs);
            }
        }
    }

    public static List<InboxEntry> listInbox(Connection c) throws SQLException {
        String q = "SELECT msg_id, gtid, source, target, pos_epoch, pos_seq, body, "
                + "tid, no_vote, phase2 FROM acta_inbox";
        List<InboxEntry> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(q);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(readInbox(rs));
            }
        }
        return out;
    }

    private static InboxEntry readInbox(ResultSet rs) throws SQLException {
        Message m = new Message(
                rs.getString("msg_id"),
                rs.getString("gtid"),
                rs.getString("source"),
                rs.getString("target"),
                rs.getBytes("body"),
                new StreamPos(rs.getLong("pos_epoch"), rs.getLong("pos_seq")));
        String nv = rs.getString("no_vote");
        String p2 = rs.getString("phase2");
        return new InboxEntry(
                m, rs.getString("tid"), nv == null ? null : NoVote.valueOf(nv), p2 == null ? null : Phase2.valueOf(p2));
    }

    /** Insert if absent. Returns true if a new row was created. */
    public static boolean insertInboxIfAbsent(Connection c, Message m) throws SQLException {
        if (getInbox(c, m.id) != null) {
            return false;
        }
        String q = "INSERT INTO acta_inbox (msg_id, gtid, source, target, pos_epoch, "
                + "pos_seq, body, tid, no_vote, phase2) VALUES (?,?,?,?,?,?,?,NULL,NULL,NULL)";
        try (PreparedStatement ps = c.prepareStatement(q)) {
            ps.setString(1, m.id);
            ps.setString(2, m.gtid);
            ps.setString(3, m.source);
            ps.setString(4, m.target);
            ps.setLong(5, m.pos.epoch);
            ps.setLong(6, m.pos.seq);
            ps.setBytes(7, m.body);
            ps.executeUpdate();
        }
        return true;
    }

    public static void setInboxTid(Connection c, String msgId, String tid) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE acta_inbox SET tid = ? WHERE msg_id = ?")) {
            ps.setString(1, tid);
            ps.setString(2, msgId);
            ps.executeUpdate();
        }
    }

    public static void setNoVote(Connection c, String msgId, NoVote v) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE acta_inbox SET no_vote = ? WHERE msg_id = ?")) {
            ps.setString(1, v.name());
            ps.setString(2, msgId);
            ps.executeUpdate();
        }
    }

    public static void setPhase2(Connection c, String msgId, Phase2 p) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE acta_inbox SET phase2 = ? WHERE msg_id = ?")) {
            ps.setString(1, p.name());
            ps.setString(2, msgId);
            ps.executeUpdate();
        }
    }

    public static void deleteInbox(Connection c, String msgId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM acta_inbox WHERE msg_id = ?")) {
            ps.setString(1, msgId);
            ps.executeUpdate();
        }
    }

    // --------------------------------------------------------------- outbox

    public static void insertOutbox(Connection c, OutboxEntry o) throws SQLException {
        String q = "INSERT INTO acta_outbox (msg_id, in_id, tid, gtid, source, target, "
                + "pos_epoch, pos_seq, body, delivery) VALUES (?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = c.prepareStatement(q)) {
            ps.setString(1, o.msg.id);
            ps.setString(2, o.inId);
            ps.setString(3, o.tid);
            ps.setString(4, o.msg.gtid);
            ps.setString(5, o.msg.source);
            ps.setString(6, o.msg.target);
            ps.setLong(7, o.msg.pos.epoch);
            ps.setLong(8, o.msg.pos.seq);
            ps.setBytes(9, o.msg.body);
            ps.setString(10, o.delivery.name());
            ps.executeUpdate();
        }
    }

    public static OutboxEntry getOutbox(Connection c, String msgId) throws SQLException {
        String q = "SELECT msg_id, in_id, tid, gtid, source, target, pos_epoch, pos_seq, "
                + "body, delivery FROM acta_outbox WHERE msg_id = ?";
        try (PreparedStatement ps = c.prepareStatement(q)) {
            ps.setString(1, msgId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return readOutbox(rs);
            }
        }
    }

    /** All retained outputs for one target, ordered by stream position. */
    public static List<OutboxEntry> listOutboxByTarget(Connection c, String target) throws SQLException {
        String q = "SELECT msg_id, in_id, tid, gtid, source, target, pos_epoch, pos_seq, "
                + "body, delivery FROM acta_outbox WHERE target = ? "
                + "ORDER BY pos_epoch ASC, pos_seq ASC";
        List<OutboxEntry> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(q)) {
            ps.setString(1, target);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(readOutbox(rs));
                }
            }
        }
        return out;
    }

    /**
     * All outbox entries produced by one input, ordered by stream position.
     * For a caller that only knows the inId (e.g. bench-server's ActaController,
     * standing in for the paper's outbox poller until the async version lands)
     * and not each output's own message id.
     */
    public static List<OutboxEntry> listOutboxByInId(Connection c, String inId) throws SQLException {
        String q = "SELECT msg_id, in_id, tid, gtid, source, target, pos_epoch, pos_seq, "
                + "body, delivery FROM acta_outbox WHERE in_id = ? "
                + "ORDER BY pos_epoch ASC, pos_seq ASC";
        List<OutboxEntry> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(q)) {
            ps.setString(1, inId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(readOutbox(rs));
                }
            }
        }
        return out;
    }

    public static List<OutboxEntry> listAllOutbox(Connection c) throws SQLException {
        String q = "SELECT msg_id, in_id, tid, gtid, source, target, pos_epoch, pos_seq, "
                + "body, delivery FROM acta_outbox";
        List<OutboxEntry> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(q);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(readOutbox(rs));
            }
        }
        return out;
    }

    /** outputsFrom(Outbox, inId) == {} check used by tryGcInbox (Figure 5). */
    public static boolean hasOutputsFrom(Connection c, String inId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM acta_outbox WHERE in_id = ? LIMIT 1")) {
            ps.setString(1, inId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static OutboxEntry readOutbox(ResultSet rs) throws SQLException {
        Message m = new Message(
                rs.getString("msg_id"),
                rs.getString("gtid"),
                rs.getString("source"),
                rs.getString("target"),
                rs.getBytes("body"),
                new StreamPos(rs.getLong("pos_epoch"), rs.getLong("pos_seq")));
        return new OutboxEntry(
                m, rs.getString("in_id"), rs.getString("tid"), Delivery.valueOf(rs.getString("delivery")));
    }

    public static void setDelivery(Connection c, String msgId, Delivery d) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE acta_outbox SET delivery = ? WHERE msg_id = ?")) {
            ps.setString(1, d.name());
            ps.setString(2, msgId);
            ps.executeUpdate();
        }
    }

    public static void deleteOutbox(Connection c, String msgId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM acta_outbox WHERE msg_id = ?")) {
            ps.setString(1, msgId);
            ps.executeUpdate();
        }
    }

    // ------------------------------------------------- epoch and watermarks

    /** Durably increment and return this service's epoch (startIncarnation). */
    public static long incrementEpoch(Connection c, String serviceId) throws SQLException {
        try (PreparedStatement ps =
                c.prepareStatement("SELECT epoch FROM acta_epoch WHERE service_id = ? FOR UPDATE")) {
            ps.setString(1, serviceId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    try (PreparedStatement ins =
                            c.prepareStatement("INSERT INTO acta_epoch (service_id, epoch) VALUES (?, 1)")) {
                        ins.setString(1, serviceId);
                        ins.executeUpdate();
                    }
                    return 1L;
                }
                long next = rs.getLong("epoch") + 1;
                try (PreparedStatement up =
                        c.prepareStatement("UPDATE acta_epoch SET epoch = ? WHERE service_id = ?")) {
                    up.setLong(1, next);
                    up.setString(2, serviceId);
                    up.executeUpdate();
                }
                return next;
            }
        }
    }

    public static StreamPos getWatermark(Connection c, String source) throws SQLException {
        try (PreparedStatement ps =
                c.prepareStatement("SELECT pos_epoch, pos_seq FROM acta_retire_watermark WHERE source = ?")) {
            ps.setString(1, source);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return StreamPos.ZERO;
                }
                return new StreamPos(rs.getLong("pos_epoch"), rs.getLong("pos_seq"));
            }
        }
    }

    /** advanceWatermark (Figure 5): monotonic max. */
    public static void advanceWatermark(Connection c, String source, StreamPos pos) throws SQLException {
        StreamPos cur = getWatermark(c, source);
        if (cur.compareTo(pos) >= 0) {
            return;
        }
        if (cur.equals(StreamPos.ZERO)) {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO acta_retire_watermark (source, pos_epoch, pos_seq) " + "VALUES (?,?,?)")) {
                ps.setString(1, source);
                ps.setLong(2, pos.epoch);
                ps.setLong(3, pos.seq);
                ps.executeUpdate();
                return;
            }
        }
        try (PreparedStatement ps =
                c.prepareStatement("UPDATE acta_retire_watermark SET pos_epoch = ?, pos_seq = ? WHERE source = ?")) {
            ps.setLong(1, pos.epoch);
            ps.setLong(2, pos.seq);
            ps.setString(3, source);
            ps.executeUpdate();
        }
    }
}
