package cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static cluster.AbstractResourceLocker.Status.LOCKED;
import static cluster.AbstractResourceLocker.Status.RETRYING;
import static cluster.AbstractResourceLocker.Status.UNLOCKED;
import static java.lang.Math.max;

/**
 * 
 * @author Jingqi Xu
 */
public abstract class AbstractResourceLocker implements ResourceLocker {
	//
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResourceLocker.class);

	//
	enum Status {LOCKED, UNLOCKED, LOCKING, RETRYING};

	//
	protected int retries = 1;
	protected final String name;
	protected final String lock;
	protected volatile Listener listener;
	protected long heartbeatInterval = 5000L;
	protected final Object guard = new Object();
	protected ScheduledExecutorService scheduler;
	protected final AtomicReference<Status> status = new AtomicReference<>(Status.UNLOCKED);

	//
	protected abstract boolean doUnlock(long timeout);
	protected abstract boolean doTryLock(long timeout);
	protected abstract boolean doRenewLock(long timeout) throws Exception;

	/**
	 *
	 */
	public AbstractResourceLocker(String lock) {
		this.lock = lock;
		this.name = "system.ha.locker";
	}

	public void start() throws Exception {
		// 1
		this.scheduler = Executors.newSingleThreadScheduledExecutor();
		this.scheduler.scheduleWithFixedDelay(new HeartbeatJob(), heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
	}

	public long stop(long timeout, TimeUnit unit) throws Exception {
		// 1
		if(isLocked()) unlock(unit.toMillis(timeout));
		return terminate(this.scheduler, timeout, unit);
	}

	/**
	 *
	 */
	public void setRetries(int retries) {
		this.retries = retries;
	}

	@Override
	public void setListener(Listener listener) {
		this.listener = listener;
	}

	public void setHeartbeatInterval(long interval) {
		this.heartbeatInterval = interval;
	}

	protected final void notifyOnLocked() {
		Listener v = this.listener; if(v != null) { v.onLocked(this); }
	}

	protected final void notifyOnUnlocked(Exception cause) {
		Listener v = listener; if(v != null) v.onUnlocked(this, cause);
	}

	/**
	 *
	 */
	@Override
	public final boolean isLocked () {
		return status.get() == LOCKED;
	}

	@Override
	public void unlock(long timeout) {
		//
		if(this.status.get() == Status.UNLOCKED) {
			throw new IllegalStateException("unlock, status: " + this.status.get());
		}

		//
		try {
			synchronized(this.guard) { doUnlock(timeout); }
		} catch(RuntimeException e) {
			LOGGER.error("[HA]failed to unlock: " + this.lock, e);
		} finally {
			if (this.status.compareAndSet(LOCKED, UNLOCKED)) notifyOnUnlocked(null);
		}
	}

	@Override
	public boolean tryLock(long timeout, boolean retry) {
		//
		if(Thread.currentThread().isInterrupted()) return false;
		if(!this.status.compareAndSet(Status.UNLOCKED, Status.LOCKING)) {
			throw new IllegalStateException("lock, status: " + (this.status.get()));
		}

		//
		boolean r = false;
		try {
			synchronized(this.guard) { r = doTryLock(timeout); }
		} catch(RuntimeException e) {
			LOGGER.error("[HA]failed to lock: " + this.lock, e);
			status.set(Status.UNLOCKED); /* LOCKING->UNLOCKED */ throw e;
		}

		//
		if (r) this.status.set(LOCKED);
		else if ((retry)) this.status.set(RETRYING); else this.status.set(UNLOCKED);
		return r;
	}

	/**
	 *
	 */
	protected class HeartbeatJob implements Runnable {
		//
		private int retries = 0;
		private long timestamp = System.currentTimeMillis();

		@Override public void run() {
			final long now = System.currentTimeMillis();
			if(status.get() == Status.LOCKED) {
				try {
					//
					if(now - this.timestamp > TimeUnit.SECONDS.toMillis(60)) {
						this.timestamp = now;
						LOGGER.info("[HA]start to renew lock: {}, status: {}", lock, status);
					}

					if (this.retries < AbstractResourceLocker.this.retries) {
						boolean r = false;
						synchronized(guard) { r = doRenewLock(heartbeatInterval); }
						if (r) retries = 0; else { retries++; }
						return;
					}

					LOGGER.error("[HA]failed to renew: " + lock);
					if (status.compareAndSet(Status.LOCKED, Status.UNLOCKED)) { notifyOnUnlocked(null); }
				} catch(Exception cause) {
					retries++;
					if(this.retries < AbstractResourceLocker.this.retries) return;
					LOGGER.error("[HA]failed to renew: " + lock, cause);
					if (status.compareAndSet(Status.LOCKED, Status.UNLOCKED)) { notifyOnUnlocked(cause); }
				}
			} else if(status.get() == Status.RETRYING) {
				try {
					//
					if(now - this.timestamp > TimeUnit.SECONDS.toMillis(60)) {
						this.timestamp = now;
						LOGGER.info("[HA]start to retry lock: {}, status: {}", lock, status);
					}

					this.retries = 0; // reset

					//
					boolean locked = false; synchronized(guard) { locked = doTryLock(heartbeatInterval); }
					if((locked) && status.compareAndSet(Status.RETRYING, Status.LOCKED)) notifyOnLocked();
				} catch(Throwable t) {
					LOGGER.error("[HA]failed to retry: " + lock, t);
				}
			}
		}
	}

	public static long terminate(ExecutorService exec, long timeout, TimeUnit unit)
			throws InterruptedException {
		// Precondition checking
		if(exec == null) return timeout;
		if(!exec.isShutdown()) exec.shutdown();

		//
		if(timeout <= 0) {
			return 0;
		} else {
			final long now = System.nanoTime();
			exec.awaitTermination(timeout, unit);
			final long elapsedTime = System.nanoTime() - now;
			return sub(timeout, unit.convert(elapsedTime, TimeUnit.NANOSECONDS));
		}
	}

	public static long sub(long v1, long v2) {
		return max(max(v1, 0) - max(v2, 0), 0);
	}
}
