package cluster;

/**
 * 
 * @author Jingqi Xu
 */
public interface ResourceLocker {
	
	/**
	 *
	 */
	boolean isLocked();

	void unlock(long timeout);

	void setListener(Listener listener);
	
	boolean tryLock(long timeout, boolean retry);
	
	interface Listener {
		
		void onLocked(ResourceLocker lock);
		
		void onUnlocked(ResourceLocker lock, Exception e);
	}
}
