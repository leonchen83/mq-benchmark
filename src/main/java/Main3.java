
import cluster.EtcdNamedLocker;
import cluster.ResourceLocker;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Baoyi Chen
 */
public class Main3 {
	public static void main(String[] args) throws Exception {
		EtcdNamedLocker locker1 = new EtcdNamedLocker("SSS");
		locker1.setPeers(new String[]{"http://192.168.1.124:2379"});
		locker1.setListener(new ResourceLocker.Listener() {
			@Override
			public void onLocked(ResourceLocker lock) {
				System.out.println("onLocked");
			}

			@Override
			public void onUnlocked(ResourceLocker lock, Exception e) {
				System.out.println("onUnlocked");
			}
		});
		locker1.setTtl(15000);
		locker1.setHeartbeatInterval(6000);
		locker1.start();
		locker1.tryLock(6000, true);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		for (int i = 0; true; i++) {
			Date date = new Date();
			if (locker1.isLocked()) {
				System.out.println("[" + format.format(date) + "] running...");
			} else {
				System.out.println("[" + format.format(date) + "] stand by...");
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}