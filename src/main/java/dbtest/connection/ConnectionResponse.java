package dbtest.connection;

import java.util.LinkedList;

/**
 * Used to encapsulate Connection objects returned from the ConnectionManager.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ConnectionResponse {
	protected boolean finished;
	protected LinkedList<Connection> connections;

	public ConnectionResponse() {
		this.finished = false;
		this.connections = new LinkedList<>();
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
	 * Add a connection object.
	 * @param connection
	 * @throws ConnectionResponseAlreadyFinishedException
	 */
	public void addConnection(Connection connection) {
		if (this.isFinished()) {
			throw new ConnectionResponseAlreadyFinishedException();
		}
		this.connections.add(connection);
	}
	
	/**
	 * @return the stored connections.
	 */
	@SuppressWarnings("unchecked")
	public Iterable<Connection> getConnections() {
		return (Iterable<Connection>) this.connections.clone();
	}
}
