package org.apache.seata.acta;

import java.util.Arrays;

/**
 * Value types for Acta (paper §3.1 execution model, §3.2 persistent state).
 *
 * Messages are immutable and globally identified. Network retries reuse the
 * original message id; a RE-EXECUTED activation assigns FRESH ids to newly
 * generated outputs. That distinction is what lets deliver() tell a live output
 * apart from one produced by an attempt the database has since rolled back.
 */
public final class ActaTypes {

    private ActaTypes() {}

    /** Inbox no-vote state. Set when local execution or prepare fails. */
    public enum NoVote {
        /** No vote is being sent yet. */
        VOTING,
        /** The no vote has been sent to the coordinator. */
        VOTED
    }

    /** Inbox phase-2 state, driven by the coordinator's decision. */
    public enum Phase2 {
        /** Coordinator decided commit; local commit not yet applied. */
        COMMITTING,
        /** Coordinator decided abort; local rollback not yet applied. */
        ABORTING,
        /** Local commit applied. */
        COMMITTED,
        /** Local rollback applied. */
        ABORTED
    }

    /** Outbox delivery state. */
    public enum Delivery {
        /** Not yet transmitted (or transmission not yet acknowledged as durable). */
        NEW,
        /** Target durably received it. */
        ACKED,
        /**
         * The target received it and ABORTED the resulting activation (see
         * ActaActivationAbortedException). Terminal, like ACKED: the message was
         * not lost, so Figure 5 retransmission does not apply, and the global
         * transaction it belongs to is being rolled back. Recovery must never
         * requeue an entry in this state -- see ActaRecovery.recoverAll's Javadoc.
         */
        FAILED
    }

    /** 2PC decision. */
    public enum Decision {
        /** The coordinator decided to commit. */
        COMMIT,
        /** The coordinator decided to abort. */
        ABORT
    }

    /** Result of recoverInput (Figure 6). */
    public enum RecoverAction {
        /** No re-execution needed; normal operation drives the rest. */
        SKIP,
        /** Safe to process the input from scratch. */
        DISPATCH
    }

    /**
     * Stream position: (epoch, sequence). Compared lexicographically, per the
     * caption of Figure 5. Epoch is bumped on every service restart so that
     * sequence numbers, which are volatile, can be safely reused.
     */
    public static final class StreamPos implements Comparable<StreamPos> {
        public final long epoch;
        public final long seq;

        public StreamPos(long epoch, long seq) {
            this.epoch = epoch;
            this.seq = seq;
        }

        public static final StreamPos ZERO = new StreamPos(0L, 0L);

        @Override
        public int compareTo(StreamPos other) {
            int c = Long.compare(this.epoch, other.epoch);
            return c != 0 ? c : Long.compare(this.seq, other.seq);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StreamPos)) {
                return false;
            }
            StreamPos p = (StreamPos) o;
            return epoch == p.epoch && seq == p.seq;
        }

        @Override
        public int hashCode() {
            return (int) (epoch * 31 + seq);
        }

        @Override
        public String toString() {
            return "(" + epoch + "," + seq + ")";
        }
    }

    /**
     * A durable message. Requests, replies and remote invocations are all
     * represented this way (§3.1).
     */
    public static final class Message {
        public final String id;
        public final String gtid;
        public final String source;
        public final String target;
        public final byte[] body;
        /** Assigned by reservePositions on the producing side. */
        public StreamPos pos;

        public Message(String id, String gtid, String source, String target, byte[] body, StreamPos pos) {
            this.id = id;
            this.gtid = gtid;
            this.source = source;
            this.target = target;
            this.body = body;
            this.pos = pos;
        }

        @Override
        public String toString() {
            return "Message{id=" + id + ", gtid=" + gtid + ", " + source + "->" + target + ", pos=" + pos + ", body="
                    + Arrays.toString(body) + "}";
        }
    }

    /** A durable inbox entry (§3.2). */
    public static final class InboxEntry {
        public final Message msg;
        public String tid;
        public NoVote noVote;
        public Phase2 phase2;

        public InboxEntry(Message msg, String tid, NoVote noVote, Phase2 phase2) {
            this.msg = msg;
            this.tid = tid;
            this.noVote = noVote;
            this.phase2 = phase2;
        }
    }

    /** A durable outbox entry (§3.2). */
    public static final class OutboxEntry {
        public final Message msg;
        public final String inId;
        public final String tid;
        public Delivery delivery;

        public OutboxEntry(Message msg, String inId, String tid, Delivery delivery) {
            this.msg = msg;
            this.inId = inId;
            this.tid = tid;
            this.delivery = delivery;
        }
    }

    /** A contiguous reserved range of sequence numbers for one target. */
    public static final class Range {
        public final String target;
        public final long begin;
        public final long end;

        public Range(String target, long begin, long end) {
            this.target = target;
            this.begin = begin;
            this.end = end;
        }
    }
}
