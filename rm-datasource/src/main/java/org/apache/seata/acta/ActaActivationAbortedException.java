package org.apache.seata.acta;

/**
 * The CONSUMER of a delivered message aborted its activation definitively --
 * its local transaction failed and it has already voted no (Figure 3 lines
 * 26-27, abortAndVote). The message was received and understood; the work
 * cannot be done.
 *
 * <h2>Why this is not a transport failure</h2>
 *
 * Figure 5's retransmission exists for messages that may have been LOST: the
 * sender cannot tell a dropped packet from a slow one, so it retries until the
 * receiver acknowledges. {@link ActaService#deliver} implements that as an
 * unbounded backoff loop around {@code transport.receive}.
 *
 * An activation that failed locally is the opposite case. Nothing was lost, the
 * receiver answered, and the answer is "this workflow cannot commit". Retrying
 * it re-executes the activation under a fresh tid until it happens to succeed,
 * which silently converts a genuine conflict into latency and hides it from the
 * client. Measured on the micro workload: Acta reported 3.7% client-visible
 * retryable failures against 10.2% for the Sonata 2-branch baseline on an
 * identical workload, and the gap was conflicts absorbed by that loop rather
 * than any difference in concurrency control.
 *
 * So this exception is the one transport outcome deliver() must NOT retry. It
 * carries the aborting message's id and the consumer's failure class so the
 * sender can record and report what happened rather than just "delivery
 * failed".
 */
public class ActaActivationAbortedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String msgId;
    private final String reason;

    public ActaActivationAbortedException(String msgId, String reason) {
        super("Acta consumer aborted activation for message " + msgId + ": " + reason);
        this.msgId = msgId;
        this.reason = reason;
    }

    /** Id of the message whose activation aborted. */
    public String msgId() {
        return msgId;
    }

    /** The consumer's failure class name, as reported in the 409 body. */
    public String reason() {
        return reason;
    }
}
