package dbtest.connection;

import java.util.logging.Logger;

/**
 * Subclasses MUST have a constructor that takes no arguments.
 * How the connection is used is implentation specific. This pro-
 * vides only guidelines on establishing the connection and commu-
 * nicating with the ConnectionManager.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public abstract class Connection {
	protected static final Logger LOGGER = Logger.getLogger(Connection.class.getName());
	
	protected final int sleepTime = 500;
	protected boolean isEstablished = false;
	
	/**
	 * Tries regularly to establish the connection via calling #tryToConnect.
	 * 
	 * Check for interruption since this probably will be executed in a concurrent context.
	 */
	public void establish() {
		// Allow interruption of thread from outside.
		while (!Thread.currentThread().isInterrupted()) {
			try {
				LOGGER.fine("Trying to connect... - " + this.getClass().getName());
				if(tryToConnect()) {
					LOGGER.fine("Connection for " + this.getClass().getName() + " successful!");
					isEstablished = true;
					return;
				} else {
					LOGGER.fine("Connection for " + this.getClass().getName() + " failed.");
				}
				Thread.sleep(sleepTime);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
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
