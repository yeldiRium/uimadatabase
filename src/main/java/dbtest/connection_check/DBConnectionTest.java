package dbtest.connection_check;

/**
 * Start a loop which calls a protected method every second, which will be
 * used to try and connect to the implementation specific database container.
 */
public abstract class DBConnectionTest implements Runnable {
	@Override
	public void run() {	
		// Allow interruption of thread from outside.
		while (!Thread.currentThread().isInterrupted()) {
			try {
				this.tryToConnect();
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
	}
	
	protected abstract void tryToConnect();
}
