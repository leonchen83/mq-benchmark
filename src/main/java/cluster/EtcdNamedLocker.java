package cluster;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.Lock;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.lock.LockResponse;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.coreos.jetcd.data.ByteSequence.fromString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Baoyi Chen
 */
public class EtcdNamedLocker extends AbstractResourceLocker {
	//
	protected long ttl;
	protected Lock locker;
	protected Lease leaser;
	protected Client client;
	protected String[] peers;
	protected volatile long lease;
	protected volatile String key;

	/**
	 *
	 */
	public EtcdNamedLocker(String lock) {
		super(lock);
	}

	/**
	 *
	 */
	public Lock getLock() {
		return this.locker;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public void setPeers(String[] peers) {
		this.peers = peers;
	}

	@Override
	public void start() throws Exception {
		this.client = Client.builder().endpoints(this.peers).build();
		locker = client.getLockClient();
		leaser = client.getLeaseClient();
		super.start();
	}

	@Override
	public long stop(long t, TimeUnit u) throws Exception {
		super.stop(t, u);
		if ((client != null)) client.close();
		return t;
	}

	@Override
	protected boolean doUnlock(long t) {
		try {
			locker.unlock(fromString(key)).get(t, MILLISECONDS);
			return true;
		} catch (Throwable e) {
			return false;
		}
	}

	@Override
	protected boolean doTryLock(long t) {
		long st = System.currentTimeMillis();
		try {
			lease = grant(ttl, t);
			ByteSequence key = fromString(lock);
			LockResponse r = locker.lock(key, lease).get(t, MILLISECONDS);
			this.key = r.getKey().toStringUtf8();
			long ed = System.currentTimeMillis();
			System.out.println("key/lease : " + this.key + ", time : " + (ed - st) + "ms");
			return true;
		} catch (Throwable e) {
			long ed = System.currentTimeMillis();
			System.out.println("failed to try lock, time : " + (ed - st) + "ms" + ", error : " + e.getMessage());
			return false;
		}
	}

	@Override
	protected void doRenewLock() throws Exception {
		leaser.keepAliveOnce(lease).get();
	}

	protected long grant(long ttl, long t) {
		return rethrow(() -> leaser.grant(MILLISECONDS.toSeconds(ttl)).get(t, MILLISECONDS).getID());
	}

	public static <T> T rethrow(Callable<T> task) {
		try {
			return task.call();
		} catch (TimeoutException t) {
			throw new RuntimeException(t);
		} catch (ExecutionException t) {
			throw new RuntimeException(t.getCause());
		} catch (InterruptedException t) {
			throw new RuntimeException(t);
		} catch (Throwable txt) {
			throw new RuntimeException(txt);
		}
	}
}
