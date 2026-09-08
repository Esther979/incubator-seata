/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seata.rm.datasource.xa;

import org.apache.seata.acta.ActaContext;
import org.apache.seata.acta.ActaFailureInjector;
import org.apache.seata.acta.ActaFailureInjector.FaultPoint;
import org.apache.seata.acta.ActaRuntime;
import org.apache.seata.acta.ActaService;
import org.apache.seata.acta.ActaTiming;
import org.apache.seata.acta.ActaTypes.Decision;
import org.apache.seata.common.DefaultValues;
import org.apache.seata.common.lock.ResourceLock;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.core.constants.DBType;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.rm.BaseDataSourceResource;
import org.apache.seata.rm.DefaultResourceManager;
import org.apache.seata.rm.datasource.util.SeataXAResource;
import org.apache.seata.sqlparser.util.JdbcConstants;
import org.postgresql.xa.PGXAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.seata.common.ConfigurationKeys.*;

/**
 * Connection proxy for XA mode.
 *
 */
public class ConnectionProxyXA extends AbstractConnectionProxyXA implements Holdable {

    /**
     * Raised when Acta's Figure 3 lines 30-38 publication block (record tid, insert
     * outbox entries) fails or refuses. Joins the XAException catch in
     * {@link #close()} so the failure gets the same cleanup and PhaseOne_Failed
     * report as a failed prepare.
     */
    public static class ActaProgressException extends Exception {

        public ActaProgressException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProxyXA.class);

    private static final int BRANCH_EXECUTION_TIMEOUT = ConfigurationFactory.getInstance()
            .getInt(XA_BRANCH_EXECUTION_TIMEOUT, DefaultValues.DEFAULT_XA_BRANCH_EXECUTION_TIMEOUT);

    private static final String DUMMY_TABLE =
            ConfigurationFactory.getInstance().getConfig(SONATA_DUMMY_TABLE, DefaultValues.DEFAULT_SONATA_DUMMY_TABLE);

    private static final int DUMMY_TABLE_SIZE = ConfigurationFactory.getInstance()
            .getInt(SONATA_DUMMY_TABLE_SIZE, DefaultValues.DEFAULT_SONATA_DUMMY_TABLE_SIZE);

    private static final int S2PL_UPDATE_RETRY_WARNING_THRESHOLD = ConfigurationFactory.getInstance()
            .getInt(
                    SONATA_S2PL_DUMMY_WRITE_RETRY_WARNING_THRESHOLD,
                    DefaultValues.DEFAULT_SONATA_S2PL_DUMMY_WRITE_RETRY_WARNING_THRESHOLD);

    private static final int SSI_HELPER_BATCH_SIZE = ConfigurationFactory.getInstance()
            .getInt(SONATA_SSI_HELPER_BATCH_SIZE, DefaultValues.DEFAULT_SONATA_SSI_HELPER_BATCH_SIZE);

    private volatile boolean currentAutoCommitStatus = true;

    private volatile XAXid xaBranchXid;

    private volatile boolean xaActive = false;

    private volatile boolean xaEnded = false;

    private volatile boolean kept = false;

    private volatile boolean rollBacked = false;

    private volatile Long branchRegisterTime = null;

    private volatile Long prepareTime = null;

    private static final Integer TIMEOUT =
            Math.max(BRANCH_EXECUTION_TIMEOUT, DefaultValues.DEFAULT_GLOBAL_TRANSACTION_TIMEOUT);

    private boolean shouldBeHeld = false;

    private final ResourceLock resourceLock = new ResourceLock();

    private volatile boolean combine = false;

    static class BranchAlreadyTerminatedException extends SQLException {
        BranchAlreadyTerminatedException(String message) {
            super(message);
        }
    }

    static boolean isClosedConnectionFailure(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException) {
                for (SQLException sqlException = (SQLException) cause;
                        sqlException != null;
                        sqlException = sqlException.getNextException()) {
                    String sqlState = sqlException.getSQLState();
                    if ("08003".equals(sqlState) || "S1009".equals(sqlState)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean isBenignMySqlXaEndFailure(XAException failure) {
        if (!DBType.MYSQL.name().equalsIgnoreCase(resource.getDbType())) {
            return false;
        }
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException) {
                SQLException sqlException = (SQLException) cause;
                if (sqlException.getErrorCode() == 1614) {
                    return true;
                }
            }
        }
        return isClosedConnectionFailure(failure);
    }

    /**
     * Constructor of Connection Proxy for XA mode.
     *
     * @param originalConnection Normal Connection from the original DataSource.
     * @param xaConnection XA Connection based on physical connection of the normal Connection above.
     * @param resource The corresponding Resource(DataSource proxy) from which the connections was created.
     * @param xid Seata global transaction xid.
     */
    public ConnectionProxyXA(
            Connection originalConnection, XAConnection xaConnection, BaseDataSourceResource resource, String xid) {
        super(originalConnection, xaConnection, resource, xid);
        this.shouldBeHeld = resource.isShouldBeHeld();
    }

    public void init() {
        try {
            this.xaResource = xaConnection.getXAResource();
            this.currentAutoCommitStatus = this.originalConnection.getAutoCommit();
            if (!currentAutoCommitStatus) {
                throw new IllegalStateException("Connection[autocommit=false] as default is NOT supported");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void keepIfNecessary() {
        if (shouldBeHeld()) {
            resource.hold(xaBranchXid.toString(), this);
        }
    }

    private void releaseIfNecessary() {
        if (shouldBeHeld()) {
            if (this.xaBranchXid != null) {
                String xaBranchXid = this.xaBranchXid.toString();
                if (isHeld()) {
                    resource.release(xaBranchXid, this);
                }
            }
        }
    }

    private void xaEnd(XAXid xaXid, int flags) throws XAException {
        if (!xaEnded) {
            xaResource.end(xaXid, flags);
            xaEnded = true;
        }
    }

    /**
     * XA commit
     * @param xid global transaction xid
     * @param branchId transaction branch id
     * @param applicationData application data
     * @throws SQLException SQLException
     */
    public void xaCommit(String xid, long branchId, String applicationData) throws XAException {
        try (ResourceLock ignored = resourceLock.obtain()) {
            XAXid xaXid = XAXidBuilder.build(xid, branchId);
            String tid = xaXid.toString();
            ActaTiming.count(ActaTiming.Site.XA_COMMIT_TOTAL);

            // Acta: Figure 4 (finish, phase-2 decision). This overload is the TC-driven commit
            // path only (ResourceManagerXA.finishBranch, RM callback thread; ActaContext is never
            // bound here), so xaResource.commit() below IS DB::finishTxn.
            //
            // isInstalled() MUST be checked before get(): "not installed" is the normal state for
            // baseline runs, and this method runs for EVERY XA commit in the process. isPending()
            // is a second, in-memory guard so branches Acta never saw cost a hash lookup, not an
            // acta_meta round trip. ActaTiming calls are measurement only.
            ActaService acta = null;
            String actaInId = null;
            if (ActaRuntime.isInstalled(resource.getDbType())) {
                acta = ActaRuntime.get(resource.getDbType());
                long t0 = System.nanoTime();
                boolean pending = acta.isPending(tid);
                ActaTiming.record(ActaTiming.Site.XA_COMMIT_IS_PENDING, System.nanoTime() - t0);
                if (pending) {
                    long t1 = System.nanoTime();
                    actaInId = acta.resolveInId(tid);
                    ActaTiming.record(ActaTiming.Site.XA_COMMIT_RESOLVE_IN_ID, System.nanoTime() - t1);
                }
            }
            if (actaInId != null) {
                long t2 = System.nanoTime();
                acta.markPhase2Start(actaInId, Decision.COMMIT);
                ActaTiming.record(ActaTiming.Site.XA_COMMIT_MARK_PHASE2_START, System.nanoTime() - t2);
                // Crash injection: no-op unless armed. The COMMIT decision is already durable at the TC.
                ActaFailureInjector.maybeCrash(actaServiceName(), FaultPoint.AFTER_COMMIT_DECIDED_BEFORE_APPLY);
            }

            if (((DataSourceProxyXA) resource).sonataSsiShimEnabled) {
                releaseHelperTxn(xaXid);
            }

            xaResource.commit(xaXid, false);

            if (((DataSourceProxyXA) resource).sonataShimEnabled) {
                forgetDummyKey(xaXid);
            }

            if (actaInId != null) {
                long t3 = System.nanoTime();
                acta.markPhase2Done(actaInId, tid, Decision.COMMIT);
                ActaTiming.record(ActaTiming.Site.XA_COMMIT_MARK_PHASE2_DONE, System.nanoTime() - t3);
            }

            releaseIfNecessary();
        }
    }

    /**
     * XA rollback
     * @param xid global transaction xid
     * @param branchId transaction branch id
     * @param applicationData application data
     */
    public void xaRollback(String xid, long branchId, String applicationData) throws XAException {
        try (ResourceLock ignored = resourceLock.obtain()) {
            // Both branches produce the same tid: XABranchXid.toString() depends only on xid+branchId.
            XAXid xaXid = this.xaBranchXid != null ? this.xaBranchXid : XAXidBuilder.build(xid, branchId);
            String tid = xaXid.toString();
            ActaTiming.count(ActaTiming.Site.XA_ROLLBACK_TOTAL);

            // Acta: Figure 4, phase-2 ABORT, TC-driven ONLY. The shared xaRollback(XAXid) below is
            // also reached from rollback()/start()/checkTimeout() as a phase-1 local rollback on a
            // branch that was never prepared -- that path must NOT be marked ABORTING, so the marking
            // lives here, around the call, not inside the shared method. Guards as in xaCommit.
            ActaService acta = null;
            String actaInId = null;
            if (ActaRuntime.isInstalled(resource.getDbType())) {
                acta = ActaRuntime.get(resource.getDbType());
                long t0 = System.nanoTime();
                boolean pending = acta.isPending(tid);
                ActaTiming.record(ActaTiming.Site.XA_ROLLBACK_IS_PENDING, System.nanoTime() - t0);
                if (pending) {
                    long t1 = System.nanoTime();
                    actaInId = acta.resolveInId(tid);
                    ActaTiming.record(ActaTiming.Site.XA_ROLLBACK_RESOLVE_IN_ID, System.nanoTime() - t1);
                }
            }
            if (actaInId != null) {
                long t2 = System.nanoTime();
                acta.markPhase2Start(actaInId, Decision.ABORT);
                ActaTiming.record(ActaTiming.Site.XA_ROLLBACK_MARK_PHASE2_START, System.nanoTime() - t2);
                ActaFailureInjector.maybeCrash(actaServiceName(), FaultPoint.AFTER_ABORT_DECIDED_BEFORE_APPLY);
            }

            // XAER_NOTA on ROLLBACK means the branch no longer exists at the RM (never prepared and
            // discarded by the DB's own crash recovery, or already rolled back). The abort IS applied,
            // so Figure 4's terminal state must still be recorded (Assumption A6: finishTxn is idempotent), otherwise
            // the inbox entry stays at
            // ABORTING forever and is never GC'd. Only XAER_NOTA is terminal; transient codes are
            // left for the TC to retry. The exception is always rethrown: Seata's protocol is unchanged.
            try {
                xaRollback(xaXid);
            } catch (XAException e) {
                if (actaInId != null && e.errorCode == XAException.XAER_NOTA) {
                    long tNota = System.nanoTime();
                    acta.markPhase2Done(actaInId, tid, Decision.ABORT);
                    ActaTiming.record(ActaTiming.Site.XA_ROLLBACK_MARK_PHASE2_DONE, System.nanoTime() - tNota);
                    LOGGER.info(
                            "Acta phase2 ABORTED for {} on XAER_NOTA: branch {} no longer exists at the RM",
                            actaInId,
                            tid);
                }
                throw e;
            }

            if (actaInId != null) {
                long t3 = System.nanoTime();
                acta.markPhase2Done(actaInId, tid, Decision.ABORT);
                ActaTiming.record(ActaTiming.Site.XA_ROLLBACK_MARK_PHASE2_DONE, System.nanoTime() - t3);
            }
        }
    }

    /**
     * XA rollback
     * @param xaXid xaXid
     * @throws XAException XAException
     */
    public void xaRollback(XAXid xaXid) throws XAException {
        boolean shouldReleaseHelper = ((DataSourceProxyXA) resource).sonataSsiShimEnabled;
        boolean helperReleased = false;

        if (shouldReleaseHelper) {
            helperReleased = releaseHelperTxn(xaXid);
        }

        try {
            xaEnd(xaXid, XAResource.TMFAIL);
        } catch (XAException e) {
            boolean shouldIgnore = false;

            // In MySQL, a branch can be in NON-EXISTING state, but still appears in `xa recover`'s output, so that
            // `xa end` would fail but `xa rollback` would still succeed. We thus suppress this xa end error.
            // Also, deadlock would automatically "roll back" branch txn, triggering error 1614 (already rolled back).
            // Still, we are able to call "XA ROLLBACK xid". So we mask this case as well.
            if (DBType.MYSQL.name().equalsIgnoreCase(resource.getDbType())) {
                Throwable cause = e.getCause();
                if (cause instanceof SQLException) {
                    int error = ((SQLException) cause).getErrorCode();
                    if (error == 1614 || (error == 1399 && cause.getMessage().contains("NON-EXISTING"))) {
                        shouldIgnore = true;
                    }
                }
            }

            // In PG, when a connection is not in the ACTIVE state or its internal xid does not equal the given xid, it
            // raises errors. This could happen when an RM restarts (all connections are fresh) and TM asks it to roll
            // back branches.
            if (e instanceof PGXAException
                    && e.getMessage().contains("tried to call end without corresponding start call")) {
                shouldIgnore = true;
            }

            if (!shouldIgnore) {
                throw e;
            }
        }
        xaResource.rollback(xaXid);

        if (((DataSourceProxyXA) resource).sonataShimEnabled) {
            if (shouldReleaseHelper && !helperReleased) {
                // If we check helper txn ID earlier than RM saving the ID, and rolling back after RM preparing the
                // branch, then the helper txn would be dangling. Since at this point we've successfully rolled back the
                // branch, the helper txn must have ID saved (if there is any) and prepared, otherwise we would fail to
                // roll back. Thus, we try again.
                // Note that, the 2nd try does not always roll back a helper txn. E.g., branch rollback caused by user
                // exceptions, branch prepare is not called at all and there is no helper txn to roll back. Thus, we
                // don't check the return boolean.
                releaseHelperTxn(xaXid);
            }
            forgetDummyKey(xaXid);
        }

        releaseIfNecessary();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (currentAutoCommitStatus == autoCommit) {
            return;
        }
        if (isReadOnly()) {
            // If it is a read-only transaction, do nothing
            currentAutoCommitStatus = autoCommit;
            return;
        }
        if (autoCommit) {
            // According to JDBC spec:
            // If this method is called during a transaction and the
            // auto-commit mode is changed, the transaction is committed.
            if (xaActive) {
                commit();
            }
        } else {
            if (this.xaBranchXid != null && currentAutoCommitStatus) {
                return;
            }
            if (xaActive) {
                throw new SQLException(
                        "should NEVER happen: setAutoCommit from true to false while xa branch is active");
            }
            // Start a XA branch
            long branchId;
            try {
                // 1. register branch to TC then get the branch message
                branchRegisterTime = System.currentTimeMillis();
                branchId = DefaultResourceManager.get()
                        .branchRegister(BranchType.XA, resource.getResourceId(), null, xid, null, null);
            } catch (TransactionException te) {
                cleanXABranchContext();
                throw new SQLException(
                        "failed to register xa branch " + xid + " since " + te.getCode() + ":" + te.getMessage(), te);
            }
            // 2. build XA-Xid with xid and branchId
            this.xaBranchXid = XAXidBuilder.build(xid, branchId);
            // Keep the Connection if necessary
            keepIfNecessary();
            try {
                start();
            } catch (XAException e) {
                cleanXABranchContext();
                throw new SQLException("failed to start xa branch " + xid + " since " + e.getMessage(), e);
            }
            // 4. XA is active
            this.xaActive = true;
        }

        currentAutoCommitStatus = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return currentAutoCommitStatus;
    }

    @Override
    public void commit() throws SQLException {
        try (ResourceLock ignored = resourceLock.obtain()) {
            if (combine) {
                return;
            }
            if (currentAutoCommitStatus || isReadOnly()) {
                // Ignore the committing on an autocommit session and read-only transaction.
                return;
            }
            if (!xaActive || this.xaBranchXid == null) {
                throw new SQLException("should NOT commit on an inactive session", SQLSTATE_XA_NOT_END);
            }
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (combine) {
            return;
        }
        if (currentAutoCommitStatus || isReadOnly()) {
            // Ignore the committing on an autocommit session and read-only transaction.
            return;
        }
        if (!xaActive || this.xaBranchXid == null) {
            throw new SQLException("should NOT rollback on an inactive session");
        }
        final String actaInId = ActaContext.getInId();
        final boolean actaEnabled = actaInId != null && ActaRuntime.isInstalled(resource.getDbType());
        try {
            if (!rollBacked) {
                try {
                    xaEnd(xaBranchXid, XAResource.TMFAIL);
                } catch (XAException e) {
                    // MySQL automatically rolls a deadlocked branch back. A statement timeout can instead make the
                    // pool close the physical connection before Spring reaches this cleanup path.
                    if (!isBenignMySqlXaEndFailure(e)) {
                        throw e;
                    }
                    LOGGER.debug("Ignore XA end for an already rolled-back MySQL branch", e);
                }
                try {
                    xaRollback(xaBranchXid);
                } catch (XAException e) {
                    // Closing the physical connection already rolls an unprepared MySQL XA branch back.
                    if (!DBType.MYSQL.name().equalsIgnoreCase(resource.getDbType()) || !isClosedConnectionFailure(e)) {
                        throw e;
                    }
                    LOGGER.debug("Ignore XA rollback on an already closed MySQL connection", e);
                }
            }
            // Branch Report to TC. Phase-1 local failure (branch never prepared): the xaRollback(XAXid)
            // above must NOT run Acta's phase-2 marking (see the TC-driven overload). VOTING/VOTED as in close().
            if (actaEnabled) {
                ActaRuntime.get(resource.getDbType()).markNoVoting(actaInId);
            }
            reportStatusToTC(BranchStatus.PhaseOne_Failed);
            if (actaEnabled) {
                ActaRuntime.get(resource.getDbType()).markNoVoted(actaInId);
            }
            LOGGER.info("{} was rollbacked", xaBranchXid);
        } catch (XAException xe) {
            throw new SQLException(
                    "Failed to end(TMFAIL) xa branch on " + xid + "-" + xaBranchXid.getBranchId() + " since "
                            + xe.getMessage(),
                    xe);
        } finally {
            cleanXABranchContext();
        }
    }

    private void start() throws XAException, SQLException {
        try (ResourceLock ignored = resourceLock.obtain()) {
            // 3. XA Start
            if (JdbcConstants.ORACLE.equals(resource.getDbType())) {
                xaResource.start(this.xaBranchXid, SeataXAResource.ORATRANSLOOSE);
            } else {
                xaResource.start(this.xaBranchXid, XAResource.TMNOFLAGS);
            }

            try {
                termination();
            } catch (SQLException e) {
                // the framework layer does not actively call ROLLBACK when setAutoCommit throws an SQL exception
                xaResource.end(this.xaBranchXid, XAResource.TMFAIL);
                xaRollback(xaBranchXid);
                // Branch Report to TC: Failed. VOTING/VOTED as in close() and rollback().
                String actaInId = ActaContext.getInId();
                boolean actaEnabled = actaInId != null && ActaRuntime.isInstalled(resource.getDbType());
                if (actaEnabled) {
                    ActaRuntime.get(resource.getDbType()).markNoVoting(actaInId);
                }
                reportStatusToTC(BranchStatus.PhaseOne_Failed);
                if (actaEnabled) {
                    ActaRuntime.get(resource.getDbType()).markNoVoted(actaInId);
                }
                throw e;
            }
        }
    }

    private synchronized void end(int flags) throws XAException, SQLException {
        xaEnd(xaBranchXid, flags);
        termination();
    }

    private void cleanXABranchContext() {
        xaEnded = false;
        branchRegisterTime = null;
        prepareTime = null;
        xaActive = false;
        if (!isHeld()) {
            xaBranchXid = null;
        }
        combine = false;
    }

    private void checkTimeout(Long now) throws XAException {
        if (now - branchRegisterTime > TIMEOUT) {
            xaRollback(xaBranchXid);
            throw new XAException("XA branch timeout error");
        }
    }

    @Override
    public void close() throws SQLException {
        try (ResourceLock ignored = resourceLock.obtain()) {
            if (combine) {
                return;
            }
            // Acta: bound by the RPC entry point before this branch's connection is closed; null for
            // any branch without an Acta activation (baselines). "Not installed" is normal, never an error.
            final String actaInId = ActaContext.getInId();
            final boolean actaEnabled = actaInId != null && ActaRuntime.isInstalled(resource.getDbType());
            try {
                if (xaActive && this.xaBranchXid != null) {
                    ActaTiming.count(ActaTiming.Site.COMMIT_TOTAL);
                    if (((DataSourceProxyXA) resource).sonataShimEnabled) {
                        sonataPrePrepare();
                    }

                    long now = System.currentTimeMillis();
                    try {
                        end(XAResource.TMSUCCESS);
                        checkTimeout(now);
                    } catch (BranchAlreadyTerminatedException terminated) {
                        // TM may decide to roll back after RM ended the branch but before RM prepared it. Preparing the
                        // branch gives the TC's rollback retry a stable XA branch to finish.
                        LOGGER.warn(terminated.getMessage());
                    } catch (SQLException sqle) {
                        // Rollback immediately before the XA Branch Context is deleted.
                        String xaBranchXid = this.xaBranchXid.toString();
                        rollback();
                        throw new SQLException(
                                "Branch " + xaBranchXid + " was rollbacked on committing since " + sqle.getMessage(),
                                SQLSTATE_XA_NOT_END,
                                sqle);
                    }
                    setPrepareTime(now);

                    // Acta: Figure 3 lines 30-38 (publication), AFTER Sonata's dummy write and end(TMSUCCESS), BEFORE
                    // xaResource.prepare(). A crash between here and prepare leaves a progress record
                    // for a branch that is not prepared -- recovery re-executes, which is correct.
                    // Persisting after prepare would leave a prepared branch with no record, and
                    // recovery would reprocess an input whose local transaction actually survived.
                    if (actaEnabled) {
                        String actaTid = xaBranchXid.toString();
                        boolean actaOk;
                        long actaT0 = System.nanoTime();
                        try {
                            actaOk = ActaRuntime.get(resource.getDbType())
                                    .recordProgress(actaInId, actaTid, ActaContext.getOutputs());
                        } catch (RuntimeException e) {
                            ActaTiming.record(ActaTiming.Site.COMMIT_RECORD_PROGRESS, System.nanoTime() - actaT0);
                            throw new ActaProgressException("Acta progress persist failed for " + actaTid, e);
                        }
                        ActaTiming.record(ActaTiming.Site.COMMIT_RECORD_PROGRESS, System.nanoTime() - actaT0);
                        if (!actaOk) {
                            throw new ActaProgressException(
                                    "Acta input " + actaInId + " is already aborting/aborted, refusing to prepare",
                                    null);
                        }
                        // Crash injection: progress is durable, prepare has not run -> DB crash recovery
                        // rolls the branch back, leaving a stale-tid outbox row.
                        ActaFailureInjector.maybeCrash(
                                actaServiceName(), FaultPoint.AFTER_OUTPUT_PERSIST_BEFORE_DELIVER);
                    }

                    int prepare = xaResource.prepare(xaBranchXid);

                    // Crash injection: prepare durably succeeded, nothing downstream has run yet.
                    if (actaEnabled) {
                        ActaFailureInjector.maybeCrash(actaServiceName(), FaultPoint.AFTER_PREPARE_BEFORE_VOTE);
                    }
                    // Based on the four databases: MySQL (8), Oracle (12c), Postgres (16), and MSSQL Server (2022),
                    // only Oracle has read-only optimization; the others do not provide read-only feedback.
                    // Therefore, the database type check can be eliminated here.
                    if (prepare == XAResource.XA_RDONLY) {
                        // Branch Report to TC: RDONLY
                        reportStatusToTC(BranchStatus.PhaseOne_RDONLY);
                    }
                }
            } catch (XAException | ActaProgressException xe) {
                if (xe instanceof ActaProgressException) {
                    // The branch was ended (TMSUCCESS) but never prepared. Discard it with XA ROLLBACK first:
                    // MySQL rejects a plain ROLLBACK while the XA branch is IDLE, which would throw out of
                    // this catch and skip the PhaseOne_Failed report below.
                    try {
                        xaResource.rollback(xaBranchXid);
                    } catch (XAException rollbackFailure) {
                        LOGGER.warn(
                                "Acta: XA rollback of unprepared branch {} failed: {}",
                                xaBranchXid,
                                rollbackFailure.getMessage());
                    }
                }
                // Some drivers (e.g., PG) do not automatically roll back and reset autocommit when failing to prepare,
                // which would cause the later reuse of the connection to fail at init(). Thus, we do it manually.
                // The Seata 2.0.0 patch applied this cleanup only to PostgreSQL. It was later generalized to all
                // databases and may be overly broad; keep the current behavior unless an actual failure requires us to
                // revisit it.
                originalConnection.rollback();
                originalConnection.setAutoCommit(true);

                if (((DataSourceProxyXA) resource).sonataShimEnabled) {
                    if (((DataSourceProxyXA) resource).sonataSsiShimEnabled) {
                        try {
                            releaseHelperTxn(xaBranchXid);
                        } catch (XAException ignored2) {
                            // On the RM side, a missing prepared helper txn is only caused by TM-initiated rollback. No
                            // action needed.
                        }
                    }
                    forgetDummyKey(xaBranchXid);
                }

                // Branch Report to TC: Failed. VOTING-before / VOTED-after (abortAndVote, Figure 3
                // lines 47-55): recoverInput's noVote==VOTING branch resends a no vote that may not have
                // reached the TC, which is dead code unless VOTING is actually recorded before the send.
                if (actaEnabled) {
                    ActaRuntime.get(resource.getDbType()).markNoVoting(actaInId);
                }
                reportStatusToTC(BranchStatus.PhaseOne_Failed);
                if (actaEnabled) {
                    ActaRuntime.get(resource.getDbType()).markNoVoted(actaInId);
                }
                throw new SQLException(
                        "Failed to end(TMSUCCESS)/prepare xa branch on " + xid + "-" + xaBranchXid.getBranchId()
                                + " since " + xe.getMessage(),
                        xe);
            } finally {
                cleanXABranchContext();
                rollBacked = false;
                if (isHeld() && shouldBeHeld()) {
                    // if kept by a keeper, just hold the connection.
                } else {
                    originalConnection.close();
                }
            }
        }
    }

    protected void closeForce() throws SQLException {
        try (ResourceLock ignored = resourceLock.obtain()) {
            Connection physicalConn = getWrappedConnection();
            if (physicalConn instanceof PooledConnection) {
                physicalConn = ((PooledConnection) physicalConn).getConnection();
            }
            // Force close the physical connection
            physicalConn.close();
            rollBacked = false;
            cleanXABranchContext();
            originalConnection.close();
            releaseIfNecessary();
        }
    }

    @Override
    public void setHeld(boolean kept) {
        this.kept = kept;
    }

    @Override
    public boolean isHeld() {
        return kept;
    }

    @Override
    public boolean shouldBeHeld() {
        return shouldBeHeld || StringUtils.isBlank(resource.getDbType());
    }

    public Long getPrepareTime() {
        return prepareTime;
    }

    private void setPrepareTime(Long prepareTime) {
        this.prepareTime = prepareTime;
    }

    private void termination() throws SQLException {
        termination(this.xaBranchXid.toString());
    }

    private void termination(String xaBranchXid) throws SQLException {
        // if it is not empty, the resource will hang and need to be terminated early
        BranchStatus branchStatus = BaseDataSourceResource.getBranchStatus(xaBranchXid);
        if (branchStatus != null) {
            releaseIfNecessary();
            throw new BranchAlreadyTerminatedException("failed xa branch " + xaBranchXid
                    + " because the global transaction has finished, branch status: " + branchStatus.getCode());
        }
    }

    /**
     * Report branch status to TC
     *
     * @param status branch status
     */
    /** Service label used by ActaFailureInjector to scope crash points to one branch type. */
    private String actaServiceName() {
        return DBType.MYSQL.name().equalsIgnoreCase(resource.getDbType()) ? "mysql-branch" : "pg-branch";
    }

    private void reportStatusToTC(BranchStatus status) {
        try {
            DefaultResourceManager.get().branchReport(BranchType.XA, xid, xaBranchXid.getBranchId(), status, null);
        } catch (TransactionException te) {
            LOGGER.warn(
                    "Failed to report XA branch {} on {}-{} since {}:{}",
                    status,
                    xid,
                    xaBranchXid.getBranchId(),
                    te.getCode(),
                    te.getMessage());
        }
    }

    /**
     * Get the lock of the current connection
     * @return the RESOURCE_LOCK
     */
    public ResourceLock getResourceLock() {
        return resourceLock;
    }

    public void setCombine(boolean combine) {
        this.combine = combine;
    }

    private int getS2plDummyKey(XAXid xid) {
        while (true) {
            int key = ThreadLocalRandom.current().nextInt(DUMMY_TABLE_SIZE);
            if (((DataSourceProxyXA) resource).ACTIVE_DUMMY_KEYS.add(key)) {
                Integer prev = ((DataSourceProxyXA) resource).XID_TO_DUMMY_KEY.putIfAbsent(xid, key);
                if (prev != null) {
                    ((DataSourceProxyXA) resource).ACTIVE_DUMMY_KEYS.remove(key);
                    // We don't know why this would happen (though programmatically possible) so we do not handle it.
                    throw new RuntimeException(
                            String.format("Global txn branch (%s) already associated with dummy key (%d)", xid, prev));
                }
                return key;
            }
        }
    }

    private int getSsiDummyKey(XAXid xid) throws SQLException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        AbstractMap.SimpleEntry<Integer, Integer> keyAndHelperId =
                ((DataSourceProxyXA) resource).RESERVED_DUMMY_KEYS_AND_HELPER_IDS.poll();
        while (keyAndHelperId == null) {
            synchronized (((DataSourceProxyXA) resource).RESERVED_DUMMY_KEYS_AND_HELPER_IDS) {
                if (!((DataSourceProxyXA) resource).RESERVED_DUMMY_KEYS_AND_HELPER_IDS.isEmpty()) {
                    keyAndHelperId = ((DataSourceProxyXA) resource).RESERVED_DUMMY_KEYS_AND_HELPER_IDS.poll();
                    continue;
                }

                // Inside this if block, reserved set is empty and only current thread is making a new one, so new keys
                // won't be added to active set, thus the following contains() test is safe.

                HashSet<Integer> newBatch = new HashSet<>();
                while (newBatch.size() < SSI_HELPER_BATCH_SIZE) {
                    int newKey = random.nextInt(DUMMY_TABLE_SIZE);
                    if (!((DataSourceProxyXA) resource).ACTIVE_DUMMY_KEYS.contains(newKey)) {
                        newBatch.add(newKey);
                    }
                }

                // We should initiate the new helper inside the synchronized block, to guarantee that helper is prepared
                // prior to all corresponding original transactions.

                int helperTxnId;
                AtomicInteger counter = new AtomicInteger(SSI_HELPER_BATCH_SIZE);
                while (true) {
                    helperTxnId = random.nextInt();
                    AtomicInteger prevCounter =
                            ((DataSourceProxyXA) resource).HELPER_ID_REF_COUNT.put(helperTxnId, counter);
                    if (prevCounter == null) {
                        break;
                    }
                }

                try (Connection helperConn = ((DataSourceProxyXA) resource).getSsiHelperConnection()) {
                    helperConn.setAutoCommit(false);

                    try (Statement helperStmt = helperConn.createStatement()) {
                        for (Integer dummyKey : newBatch) {
                            try (ResultSet rs = helperStmt.executeQuery(
                                    "select count(*) from \"" + DUMMY_TABLE + "\" where key=" + dummyKey)) {
                                if (rs.next()) {
                                    rs.getInt(1); // to make sure read is executed in the DB
                                }
                            }
                        }

                        int ret = helperStmt.executeUpdate("prepare transaction '" + helperTxnId + "'");
                        if (ret != 0) {
                            throw new RuntimeException(String.format(
                                    "PostgreSQL txn prepare returned %d, which should be 0 instead", ret));
                        }
                    }
                }

                // Mark them all active
                ((DataSourceProxyXA) resource).ACTIVE_DUMMY_KEYS.addAll(newBatch);

                // Make them visible in random order
                List<Integer> asList = new ArrayList<>(newBatch);
                Collections.shuffle(asList);
                int firstKey = asList.remove(0); // take one for ourselves to avoid loop again
                for (int otherKey : asList) {
                    ((DataSourceProxyXA) resource)
                            .RESERVED_DUMMY_KEYS_AND_HELPER_IDS.add(
                                    new AbstractMap.SimpleEntry<>(otherKey, helperTxnId));
                }

                keyAndHelperId = new AbstractMap.SimpleEntry<>(firstKey, helperTxnId);
            }
        }

        // associate xid with dummy key
        Integer prevDummy = ((DataSourceProxyXA) resource).XID_TO_DUMMY_KEY.putIfAbsent(xid, keyAndHelperId.getKey());
        if (prevDummy != null) {
            throw new RuntimeException(
                    String.format("Global txn branch (%s) already associated with dummy key (%d)", xid, prevDummy));
        }

        // associate xid with helper id
        Integer prevHelper =
                ((DataSourceProxyXA) resource).XID_TO_HELPER_ID.putIfAbsent(xid, keyAndHelperId.getValue());
        if (prevHelper != null) {
            throw new RuntimeException(String.format(
                    "Global txn branch (%s) already associated with helper txn ID (%d)", xid, prevHelper));
        }

        return keyAndHelperId.getKey();
    }

    private void forgetDummyKey(XAXid xid) {
        Integer prev = ((DataSourceProxyXA) resource).XID_TO_DUMMY_KEY.remove(xid);
        if (prev == null) {
            // Possible if TM initiated a rollback, and RM is yet to prepare the branch
            return;
        }

        boolean recorded = ((DataSourceProxyXA) resource).ACTIVE_DUMMY_KEYS.remove(prev);
        if (!recorded) {
            throw new RuntimeException(String.format(
                    "Dummy key (%d) mapped to global txn branch (%s) not found in active dummy key set", prev, xid));
        }
    }

    private boolean releaseHelperTxn(XAXid xid) throws XAException {
        Integer helperTxnId = ((DataSourceProxyXA) resource).XID_TO_HELPER_ID.remove(xid);

        if (helperTxnId == null) {
            // Possible if TM initiated a rollback, and RM is yet to prepare the branch
            return false;
        }

        AtomicInteger counter = ((DataSourceProxyXA) resource).HELPER_ID_REF_COUNT.get(helperTxnId);
        if (counter == null) {
            throw new RuntimeException("Reference counter for helper txn (" + helperTxnId + ") not found");
        }

        int currentUsage = counter.decrementAndGet();
        if (currentUsage == 0) {
            ((DataSourceProxyXA) resource).HELPER_ID_REF_COUNT.remove(helperTxnId);
            try (Statement stmt = originalConnection.createStatement()) { // do not use wrapped xa stmt
                stmt.executeUpdate("rollback prepared '" + helperTxnId + "'");
            } catch (SQLException e) {
                if ("42704".equals(e.getSQLState())) {
                    LOGGER.error("Helper txn ({}) not prepared; should be possible with helper sharing", helperTxnId);
                    // This is caused by TM actively rolling back the branch while the branch is preparing. We return an
                    // XAException to notify TM to try again later. Extremely rare, but still possible (e.g., if helper
                    // batch size=1).
                    XAException xe = new XAException("Helper txn (" + helperTxnId + ") not yet prepared, try again");
                    xe.errorCode = XAException.XA_RETRY;
                    throw xe;
                }

                LOGGER.error("Unexpected SQLException while rolling back helper txn: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    private void sonataPrePrepare() throws SQLException, XAException {
        // For an S2PL DB, we add a dummy write; for an SSI DB, we add a helper txn + a dummy write. Then,
        // original prepare logic resumes.
        if (DBType.MYSQL.name().equalsIgnoreCase(resource.getDbType())) {
            int key = getS2plDummyKey(xaBranchXid);
            int value = ThreadLocalRandom.current().nextInt();

            try (Statement stmt = createStatement()) {
                int retry = 0;
                int affected = 0;
                // Note: affected=1 for insert; =2 for update; =0 for update but value unchanged
                while (affected == 0) {
                    affected =
                            stmt.executeUpdate("insert into `" + DUMMY_TABLE + "` (`key`, value) values (" + key + ","
                                    + value + ") on duplicate key update value="
                                    + value);
                    retry++;
                    if (retry > S2PL_UPDATE_RETRY_WARNING_THRESHOLD) {
                        LOGGER.warn(
                                "MySQL random dummy writes generated conflict with existing rows in {} consecutive attempts! This is super unlikely to occur, please investigate.",
                                retry);
                    }
                }
            }
        } else if (DBType.POSTGRESQL.name().equalsIgnoreCase(resource.getDbType())) {
            int key = getSsiDummyKey(xaBranchXid);
            int value = ThreadLocalRandom.current().nextInt();

            try (Statement stmt = createStatement()) {
                // Unlike MySQL, PostgreSQL does not distinguish matched but value-unchanged rows.
                int affected;
                try {
                    affected = stmt.executeUpdate(
                            "insert into \"" + DUMMY_TABLE + "\" (key, value) values (" + key + "," + value
                                    + ") on conflict (key) do update set value="
                                    + value);
                } catch (SQLException e) {
                    if ("40001".equals(e.getSQLState())) {
                        throw new XAException("PostgreSQL dummy write of global txn branch (" + xaBranchXid
                                + ") failed due to serialization failure");
                    }

                    LOGGER.error("Unexpected SQLException while PG dummy write: {}", e.getMessage());
                    throw e;
                }
                if (affected != 1) {
                    // We don't know why this would happen (though programmatically possible) so we do not
                    // handle it.
                    throw new RuntimeException(String.format(
                            "PostgreSQL dummy write affected %d row(s), which should be 1 instead", affected));
                }
            }
        }
    }
}
