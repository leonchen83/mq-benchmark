package cluster;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.Lock;
import com.coreos.jetcd.common.exception.EtcdException;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.coreos.jetcd.data.ByteSequence.fromString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Baoyi Chen
 */
public class EtcdNamedLocker extends AbstractResourceLocker {
    //
    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdNamedLocker.class);
    
    protected long ttl;
    protected String[] peers;
    protected XClient client;
    protected volatile Long lease;
    protected volatile String key;
    
    /**
     *
     */
    public EtcdNamedLocker(String lock) {
        super(lock);
    }
    
    public void setTtl(long ttl) {
        this.ttl = ttl;
    }
    
    public void setPeers(String[] peers) {
        this.peers = peers;
    }
    
    @Override
    public void start() throws Exception {
        this.client = new XClient(peers); this.client.start(); super.start();
    }
    
    @Override
    public long stop(long t, TimeUnit u) throws Exception {
        super.stop(t, u); return this.client.stop(t, u);
    }
    
    @Override
    protected boolean doTryLock(long timeout) {
        try {
            if (!lease(timeout)) return false;
            key = client.lock(lock, lease).get(timeout, MILLISECONDS); return true;
        } catch (Throwable root) {
            final String cause = getRootMessage(root);
            if (isCausedBy(root, TimeoutException.class)) {
                LOGGER.error("failed to lock[TIMEOUT] lease: {}", lease);
            } else if (isCausedBy(root, EtcdException.class)) {
                LOGGER.error("failed to lock[ETCD] lease: {}, cause: {}", lease, cause);
            } else {
                LOGGER.error("failed to lock[INTERNAL_ERROR] lease: {}", lease, (root));
            }
            revoke(timeout); lease = null; return false;
        }
    }
    
    @Override
    protected boolean doUnlock(long timeout) {
        try {
            if (key == null) return false;
            return client.unlock(key).get(timeout, MILLISECONDS);
        } catch (Throwable root) {
            final String cause = getRootMessage(root);
            if (isCausedBy(root, TimeoutException.class)) {
                LOGGER.error("failed to unlock[TIMEOUT] lease: {}", lease);
            } else if (isCausedBy(root, EtcdException.class)) {
                LOGGER.error("failed to unlock[ETCD] lease: {}, cause: {}", lease, cause);
            } else {
                LOGGER.error("failed to unlock[INTERNAL_ERROR] lease: {}", lease, (root));
            }
            return false;
        }
    }
    
    @Override
    protected boolean doRenewLock(long timeout) throws Exception {
        try {
            if (lease == null) return false;
            return client.renewal(lease).get(timeout, MILLISECONDS);
        } catch (Throwable root) {
            final String cause = getRootMessage(root);
            if (isCausedBy(root, TimeoutException.class)) {
                LOGGER.error("failed to renew[TIMEOUT] lease: {}", lease);
            } else if (isCausedBy(root, EtcdException.class)) {
                LOGGER.error("failed to renew[ETCD] lease: {}, cause: {}", lease, cause);
            } else {
                LOGGER.error("failed to renew[INTERNAL_ERROR] lease: {}", lease, (root));
            }
            return false;
        }
    }
    
    /**
     *
     */
    protected boolean lease(long timeout) {
        try {
            if (lease != null) return true;
            return (lease = client.lease(ttl).get(timeout, MILLISECONDS)) != null;
        } catch (Throwable root) {
            final String cause = getRootMessage(root);
            if (isCausedBy(root, TimeoutException.class)) {
                LOGGER.error("failed to lease[TIMEOUT] lease: {}", lease);
            } else if (isCausedBy(root, EtcdException.class)) {
                LOGGER.error("failed to lease[ETCD] lease: {}, cause: {}", lease, cause);
            } else {
                LOGGER.error("failed to lease[INTERNAL_ERROR] lease: {}", lease, (root));
            }
            return false;
        }
    }
    
    /**
     *
     */
    protected boolean revoke(long timeout) {
        try {
            if (lease == null) return false;
            return client.revoke(lease).get(timeout, MILLISECONDS);
        } catch (Throwable root) {
            final String cause = getRootMessage(root);
            if (isCausedBy(root, TimeoutException.class)) {
                LOGGER.error("failed to revoke[TIMEOUT] lease: {}", lease);
            } else if (isCausedBy(root, EtcdException.class)) {
                LOGGER.error("failed to revoke[ETCD] lease: {}, cause: {}", lease, cause);
            } else if (isCausedBy(root, StatusRuntimeException.class)) {
                StatusRuntimeException ex = (StatusRuntimeException)(getRootCause(root));
                if(ex.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) return true;
                LOGGER.error("failed to revoke[INTERNAL_ERROR] lease: {}", lease, (root));
            } else {
                LOGGER.error("failed to revoke[INTERNAL_ERROR] lease: {}", lease, (root));
            }
            return false;
        }
    }

    protected static final Throwable root(final Throwable tx) {
        Throwable r = ExceptionUtils.getRootCause(tx); return r == null ? tx : r;
    }

    public static final Throwable getRootCause(final Throwable tx) {
        if(tx == null) return null; final Throwable root = root(tx); return root;
    }

    public static final String getRootMessage(final Throwable tx) {
        if(tx == null) return null; Throwable r = root(tx); return r.getMessage();
    }

    /**
     *
     */
    public static final boolean isCausedBy(Throwable tx, final Class<? extends Throwable> cause) {
        for(; tx != null; tx = tx.getCause()) if(cause.isAssignableFrom(tx.getClass())) return true; return false;
    }
    
    /**
     *
     */
    protected static class XClient {
    
        protected Lock lock;
        protected Lease lease;
        protected Client client;
        protected String[] peers;
        
        protected XClient(String[] peers) {
            this.peers = peers;
        }
    
        public void start() throws Exception {
            this.client = Client.builder().endpoints(peers).build();
            lock = client.getLockClient(); lease = client.getLeaseClient();
        }

        public long stop(long timeout, TimeUnit unit) throws Exception {
            this.client.close(); return timeout;
        }
    
        public CompletableFuture<String> lock(String name, long lease) {
            final CompletableFuture<String> future = new CompletableFuture<>();
            this.lock.lock(fromString(name), lease).whenComplete((r, t) -> {
                String v = r == null ? null : r.getKey().toStringUtf8();
                if (t != null) future.completeExceptionally(t); else future.complete(v);
            });
            return future;
        }
    
        public CompletableFuture<Long> lease(long ttl) {
            final CompletableFuture<Long> future = new CompletableFuture<>();
            final long s = MILLISECONDS.toSeconds(ttl);
            this.lease.grant(s).whenComplete((r, t) -> {
                Long v = r == null ? null : r.getID();
                if (t != null) future.completeExceptionally(t); else future.complete(v);
            });
            return future;
        }
    
        public CompletableFuture<Boolean> unlock(String key) {
            final CompletableFuture<Boolean> future = new CompletableFuture<>();
            this.lock.unlock(fromString(key)).whenComplete((r, t) -> {
                if (t != null) future.completeExceptionally(t); else future.complete(r != null);
            });
            return future;
        }
    
        public CompletableFuture<Boolean> revoke(long lease) {
            final CompletableFuture<Boolean> future = new CompletableFuture<>();
            this.lease.revoke(lease).whenComplete((r, t) -> {
                if (t != null) future.completeExceptionally(t); else future.complete(r != null);
            });
            return future;
        }
    
        public CompletableFuture<Boolean> renewal(long lease) {
            final CompletableFuture<Boolean> future = new CompletableFuture<>();
            this.lease.keepAliveOnce(lease).whenComplete((r, t) -> {
                if (t != null) future.completeExceptionally(t); else future.complete(r != null && r.getTTL() > 0);
            });
            return future;
        }
    }
}
