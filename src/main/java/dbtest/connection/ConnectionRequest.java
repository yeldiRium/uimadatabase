package dbtest.connection;

import java.util.HashSet;
import java.util.Set;

/**
 * The ConnectionRequest is used to transfer a number of Connection classes to the ConnectionManager.
 * It tells the Manager, which Connections should be prepared and who should be called with them.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ConnectionRequest {
	private HashSet<Class<?extends Connection>> connections;
	
	/**
	 * Constructs the ConnectionRequest from an endpoint and creates an empty list for the connection classes.
	 * @param responseEndpoint
	 */
	public ConnectionRequest() {
		this.connections = new HashSet<>(); 
	}
	
	/**
	 * Constructs the ConnectionRequest from an endpoint and a list of connection classes.
	 * @param responseEndpoint
	 * @param connectionClasses
	 */
	@SafeVarargs
	public ConnectionRequest(Class<?extends Connection>... connectionClasses) {
		this.connections = new HashSet<>();
		for(Class<?extends Connection> connectionClass: connectionClasses) {
			this.connections.add(connectionClass);
		}
	}
	
	/**
	 * Adds a connectionClass to the collection.
	 * @param connectionClass
	 */
	public void addRequestedConnection(Class<?extends Connection> connectionClass) {
		this.connections.add(connectionClass);
	}
	
	/**
	 * @return the iterable of connection classes.
	 */
	@SuppressWarnings("unchecked")
	public Set<Class<?extends Connection>> getRequestedConnections() {
		return (Set<Class<? extends Connection>>) this.connections.clone();
	}
}
