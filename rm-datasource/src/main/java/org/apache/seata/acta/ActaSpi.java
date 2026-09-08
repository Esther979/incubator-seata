package org.apache.seata.acta;

import org.apache.seata.acta.ActaTypes.Message;

import java.sql.Connection;
import java.util.List;

/**
 * The three things Acta must be wired into: the 2PC coordinator, the
 * application handler, and the transport.
 */
public final class ActaSpi {

    private ActaSpi() {}

    /**
     * 2PC coordinator client.
     *
     * Seata mapping:
     *   register(gtid, participantId) -> branchRegister. Returns false when the
     *       global transaction is already Rollbacking/Rollbacked, which Seata's
     *       TC reports as a registration failure. THE PARTICIPANT ID MUST BE THE
     *       INPUT MESSAGE ID (§3.3) -- that is what makes registration idempotent
     *       across re-execution and network retries.
     *   voteYes  -> branchReport(BranchStatus.PhaseOne_Done)
     *   voteNo   -> branchReport(BranchStatus.PhaseOne_Failed)
     *
     * The reverse direction (coordinator -> participant) arrives as
     * branchCommit / branchRollback and should call ActaService.finish().
     */
    public interface TwoPcClient {
        boolean register(String gtid, String participantId);

        void voteYes(String gtid, String participantId);

        void voteNo(String gtid, String participantId);
    }

    /**
     * The application handler for one activation (§3.1).
     *
     * Contract:
     *  - All local data access MUST go through {@code appConn}, which is bound to
     *    the XA branch Acta opened. Handlers may not perform external side
     *    effects that bypass the local database or Acta-managed invocations.
     *  - Remote invocations and the handler reply are returned as messages and
     *    are NOT transmitted here. Acta persists them first, then delivers.
     *  - Throwing signals a local execution failure and leads to a no vote.
     */
    public interface Handler {
        List<Message> execute(Connection appConn, Message input) throws Exception;
    }

    /**
     * Sender side of the transport. Must not return until the target has
     * DURABLY received the message (i.e. the target's receive() has committed).
     * Should throw on any failure so deliver() retries.
     */
    public interface Transport {
        void receive(String target, Message msg) throws Exception;

        /**
         * RPC::advanceWatermark (Figure 5 line 114). Tells {@code target} that
         * this service's stream is retired through {@code pos}. Must not return
         * until the target has durably recorded it, since the sender deletes
         * outbox entries only after this returns.
         */
        void advanceWatermark(String target, String source, ActaTypes.StreamPos pos) throws Exception;
    }
}
