package com.mysql.cj;

import com.mysql.cj.Query.CancelStatus;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import core.framework.db.CloudAuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

public class CancelQueryTaskImpl extends TimerTask implements CancelQueryTask {
    // in cloud env, global timer is better and with fewer threads running
    public static final Timer TIMER = new Timer("mysql-query-timer", true);

    private static final Logger LOGGER = LoggerFactory.getLogger(CancelQueryTaskImpl.class);

    Query queryToCancel;
    Throwable caughtWhileCancelling = null;

    public CancelQueryTaskImpl(Query query) {
        this.queryToCancel = query;
    }

    @Override
    public boolean cancel() {
        boolean res = super.cancel();
        this.queryToCancel = null;
        return res;
    }

    @Override
    public void run() {
        Thread.ofVirtual().start(() -> {
            Query query = queryToCancel;
            if (query == null) {
                return;
            }
            NativeSession session = (NativeSession) query.getSession();
            if (session == null) {
                return;
            }

            try {
                killQuery(query, session);
            } catch (Throwable t) {
                caughtWhileCancelling = t;
            } finally {
                setQueryToCancel(null);
            }
        });
    }

    private void killQuery(Query localQueryToCancel, NativeSession session) throws IOException, CJException {
        ReentrantLock lock = localQueryToCancel.getCancelTimeoutMutex();
        lock.lock();
        try {
            long origConnId = session.getThreadId();
            HostInfo hostInfo = session.getHostInfo();
            String database = hostInfo.getDatabase();

            CloudAuthProvider authProvider = authProvider(hostInfo);
            String user = authProvider != null ? authProvider.user() : hostInfo.getUser();
            String password = authProvider != null ? authProvider.accessToken() : hostInfo.getPassword();

            LOGGER.warn("kill query due to timeout, processId={}, query={}", origConnId, localQueryToCancel);

            NativeSession newSession = null;
            try {
                newSession = new NativeSession(hostInfo, session.getPropertySet());
                newSession.connect(hostInfo, user, password, database, 30000);
                newSession.getProtocol().sendCommand(new NativeMessageBuilder(newSession.getServerSession().supportsQueryAttributes())
                        .buildComQuery(newSession.getSharedSendPacket(), "KILL QUERY " + origConnId), false, 0);
            } finally {
                close(newSession);
            }
            localQueryToCancel.setCancelStatus(CancelStatus.CANCELED_BY_TIMEOUT);
        } finally {
            lock.unlock();
        }
    }

    private CloudAuthProvider authProvider(HostInfo hostInfo) {
        if (hostInfo.getHostProperties().get(CloudAuthProvider.Provider.CLOUD_AUTH) != null) {
            return CloudAuthProvider.Provider.get();
        }
        return null;
    }

    private void close(NativeSession session) {
        try {
            if (session != null) session.forceClose();
        } catch (Throwable e) {
            LOGGER.warn("failed to close session", e);
        }
    }

    @Override
    public Throwable getCaughtWhileCancelling() {
        return this.caughtWhileCancelling;
    }

    @Override
    public void setQueryToCancel(Query queryToCancel) {
        this.queryToCancel = queryToCancel;
    }
}
