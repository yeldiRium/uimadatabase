package dbtest.connection;

/**
 * Subclasses MUST have a constructor that takes no arguments.
 * How the connection is used is implentation specific. This pro-
 * vides only guidelines on establishing the connection and commu-
 * nicating with the ConnectionManager.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public abstract class Connection {
	protected final int sleepTime = 500;
	protected boolean isEstablished = false;
	protected Thread connectionEstablisher;
	
	protected class ConnectionEstablisher implements Runnable {
		@Override
		public void run() {
			// Allow interruption of thread from outside.
			while (!Thread.currentThread().isInterrupted()) {
				try {
					if(tryToConnect()) {
						isEstablished = true;
						Thread.currentThread().interrupt();
					}
					Thread.sleep(sleepTime);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
	/**
	 * Tries regularly to establish the connection via calling #tryToConnect.
	 */
	public void establish() {
		this.connectionEstablisher = new Thread(new ConnectionEstablisher());
		this.connectionEstablisher.start();
	}
	
	/**
	 * Tries to connect to the implementation-specific database.
	 * Returns true, if the connection was established and can be used.
	 * @return
	 */
	protected abstract boolean tryToConnect();
	
	/**
	 * @return
	 */
	public boolean isEstablished() {
		return this.isEstablished;
	}
}
