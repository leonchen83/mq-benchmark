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
	protected final String name;
	protected final String lock;
	protected volatile Listener listener;
	protected long heartbeatInterval = 5000L;
	protected final Object guard = new Object();
	protected ScheduledExecutorService scheduler;
	protected final AtomicReference<Status> status = new AtomicReference<>(Status.UNLOCKED);

	//
	protected void doRenewLock() throws Exception {}
	protected abstract boolean doUnlock(long timeout);
	protected abstract boolean doTryLock(long timeout);

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

	@Override
	public void setListener(Listener listener) {
		this.listener = listener;
	}

	public void setHeartbeatInterval(long interval) {
		this.heartbeatInterval = interval;
	}

	protected final void notifyOnLocked() {
		final Listener l = this.listener; if(l != null) l.onLocked(this);
	}

	protected final void notifyOnUnlocked(Exception e) {
		final Listener l = this.listener; if(l != null) l.onUnlocked(this, e);
	}

	/**
	 *
	 */
	@Override
	public final boolean isLocked() {
		return this.status.get() == Status.LOCKED;
	}

	@Override
	public void unlock(long timeout) {
		//
		if(this.status.get() == Status.UNLOCKED) {
			throw new IllegalStateException("status: " + this.status.get());
		}

		//
		try {
			synchronized(this.guard) { doUnlock(timeout); }
		} catch(RuntimeException e) {
			LOGGER.error("failed to unlock: " + this.lock, e);
		} finally {
			if(this.status.compareAndSet(LOCKED, UNLOCKED)) notifyOnUnlocked(null);
		}
	}

	@Override
	public boolean tryLock(long timeout, boolean retry) {
		//
		if(!this.status.compareAndSet(Status.UNLOCKED, Status.LOCKING)) {
			throw new IllegalStateException("status: " + this.status.get());
		}

		//
		boolean r = false;
		if(Thread.currentThread().isInterrupted()) return false;
		try {
			synchronized(this.guard) { r = doTryLock(timeout); }
		} catch(RuntimeException e) {
			this.status.set(Status.UNLOCKED); throw e;
		}
		//
		if(r) status.set(LOCKED); else if(retry) status.set(RETRYING); else status.set(UNLOCKED);
		return r;
	}

	/**
	 *
	 */
	protected class HeartbeatJob implements Runnable {
		//
		private volatile long timestamp = System.currentTimeMillis();

		@Override
		public void run() {
			final long now = System.currentTimeMillis();
			if(status.get() == Status.LOCKED) {
				try {
					//
					if(now - this.timestamp > TimeUnit.SECONDS.toMillis(60)) {
						this.timestamp = now;
						LOGGER.info("[HA]start to renew lock: {}, status: {}", lock, status);
					}

					//
					synchronized(guard) { doRenewLock(); } // Try to renew lock
				} catch(Exception e) {
					LOGGER.error("[HA]failed to renew lock: " + lock, e);
					if(status.compareAndSet(Status.LOCKED, Status.UNLOCKED)) notifyOnUnlocked(e);
				}
			} else if(status.get() == Status.RETRYING) {
				try {
					//
					if(now - this.timestamp > TimeUnit.SECONDS.toMillis(60)) {
						this.timestamp = now;
						LOGGER.info("[HA]start to retry lock: {}, status: {}", lock, status);
					}

					//
					boolean r = false;
					synchronized(guard) { r = doTryLock(heartbeatInterval); } // Try to lock
					if(r && status.compareAndSet(Status.RETRYING, Status.LOCKED)) notifyOnLocked();
				} catch(Exception e) {
					LOGGER.error("[HA]failed to retry lock: " + lock, e);
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
