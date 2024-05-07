/*
 * Copyright (c) 2017, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.CJTimeoutException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.OperationCancelledException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractQuery implements Query {

    public NativeSession session = null;

    protected RuntimeProperty<Integer> maxAllowedPacket;

    /** The character encoding to use (if available) */
    protected String charEncoding = null;

    /** Mutex to prevent race between returning query results and noticing that query has been timed-out or cancelled. */
    protected ReentrantLock cancelTimeoutMutex = new ReentrantLock();

    private CancelStatus cancelStatus = CancelStatus.NOT_CANCELED;

    /** The timeout for a query */
    protected long timeoutInMillis = 0L;

    /** Holds batched commands */
    protected List<QueryBindings> batchedArgs;

    /** Currently executing a statement? */
    protected final AtomicBoolean statementExecuting = new AtomicBoolean(false);

    /** Has clearWarnings() been called? */
    protected boolean clearWarningsCalled = false;

    public AbstractQuery(NativeSession sess) {
        this.session = sess;
        this.maxAllowedPacket = sess.getPropertySet().getIntegerProperty(PropertyKey.maxAllowedPacket);
        this.charEncoding = sess.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
    }

    @Override
    public void setCancelStatus(CancelStatus cs) {
        this.cancelStatus = cs;
    }

    @Override
    public void checkCancelTimeout() throws CJException {
        cancelTimeoutMutex.lock();
        try {
            if (this.cancelStatus != CancelStatus.NOT_CANCELED) {
                CJException cause = this.cancelStatus == CancelStatus.CANCELED_BY_TIMEOUT ? new CJTimeoutException() : new OperationCancelledException();
                resetCancelledState();
                throw cause;
            }
        } finally {
            cancelTimeoutMutex.unlock();
        }
    }

    @Override
    public void resetCancelledState() {
        cancelTimeoutMutex.lock();
        try {
            this.cancelStatus = CancelStatus.NOT_CANCELED;
        } finally {
            cancelTimeoutMutex.unlock();
        }
    }

    @Override
    public <T extends Resultset, M extends Message> ProtocolEntityFactory<T, M> getResultSetFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NativeSession getSession() {
        return this.session;
    }

    @Override
    public ReentrantLock getCancelTimeoutMutex() {
        return this.cancelTimeoutMutex;
    }

    @Override
    public void closeQuery() {
        this.session = null;
    }

    public void addBatch(QueryBindings batch) {
        if (this.batchedArgs == null) {
            this.batchedArgs = new ArrayList<>();
        }
        this.batchedArgs.add(batch);
    }

    public List<QueryBindings> getBatchedArgs() {
        return this.batchedArgs == null ? null : Collections.unmodifiableList(this.batchedArgs);
    }

    public void clearBatchedArgs() {
        if (this.batchedArgs != null) {
            this.batchedArgs.clear();
        }
    }

    @Override
    public long getTimeoutInMillis() {
        return this.timeoutInMillis;
    }

    @Override
    public void setTimeoutInMillis(long timeoutInMillis) {
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    public CancelQueryTask startQueryTimer(Query query, long timeout) {
        if (timeout > 0) {
            var timeoutTask = new CancelQueryTaskImpl(query);
            CancelQueryTaskImpl.TIMER.schedule(timeoutTask, timeout);
            return timeoutTask;
        }
        return null;
    }

    @Override
    public void stopQueryTimer(CancelQueryTask timeoutTask, boolean rethrowCancelReason, boolean checkCancelTimeout) throws CJException {
        if (timeoutTask != null) {
            timeoutTask.cancel();

            if (rethrowCancelReason && timeoutTask.getCaughtWhileCancelling() != null) {
                Throwable t = timeoutTask.getCaughtWhileCancelling();
                throw ExceptionFactory.createException(t.getMessage(), t);
            }

            CancelQueryTaskImpl.TIMER.purge();

            if (checkCancelTimeout) {
                checkCancelTimeout();
            }
        }
    }

    @Override
    public AtomicBoolean getStatementExecuting() {
        return this.statementExecuting;
    }

    @Override
    public boolean isClearWarningsCalled() {
        return this.clearWarningsCalled;
    }

    @Override
    public void setClearWarningsCalled(boolean clearWarningsCalled) {
        this.clearWarningsCalled = clearWarningsCalled;
    }

    @Override
    public void statementBegins() {
        this.clearWarningsCalled = false;
        this.statementExecuting.set(true);
    }

}
