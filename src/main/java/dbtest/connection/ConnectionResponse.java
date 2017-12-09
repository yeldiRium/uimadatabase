package dbtest.connection;

import java.util.HashSet;
import java.util.Set;

import dbtest.connection.exception.ConnectionResponseAlreadyFinishedException;

/**
 * Used to encapsulate Connection objects returned from the ConnectionManager.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ConnectionResponse {
	protected boolean finished;
	protected HashSet<Connection> connections;

	public ConnectionResponse() {
		this.finished = false;
		this.connections = new HashSet<>();
	}
	
	/**
	 * Finishes the Response and disallows adding any further connections.
	 */
	public void finish() {
		this.finished = true;
	}
	
	/**
	 * Returns true, if the Response was finished.
	 * @return
	 */
	public boolean isFinished() {
		return this.finished;
	}
	
	/**
	 * Add a connection object. If it is already there, nothing happens.
	 * @param connection
	 * @throws ConnectionResponseAlreadyFinishedException
	 */
	public void addConnection(Connection connection) {
		if (this.isFinished()) {
			throw new ConnectionResponseAlreadyFinishedException();
		}
		if (!this.connections.contains(connection)) {
			this.connections.add(connection);
		}
	}
	
	/**
	 * @return the stored connections.
	 */
	@SuppressWarnings("unchecked")
	public Set<Connection> getConnections() {
		return (Set<Connection>) this.connections.clone();
	}
}
