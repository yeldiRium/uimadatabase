package dbtest.connection;

import java.util.LinkedList;

/**
 * The ConnectionRequest is used to transfer a number of Connection classes to the ConnectionManager.
 * It tells the Manager, which Connections should be prepared and who should be called with them.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ConnectionRequest {
	private AcceptsConnectionResponse responseEndpoint;
	private LinkedList<Class<?extends Connection>> connections;
	
	/**
	 * Constructs the ConnectionRequest from an endpoint and creates an empty list for the connection classes.
	 * @param responseEndpoint
	 */
	public ConnectionRequest(AcceptsConnectionResponse responseEndpoint) {
		this.responseEndpoint = responseEndpoint;
		this.connections = new LinkedList<>(); 
	}
	
	/**
	 * Constructs the ConnectionRequest from an endpoint and a list of connection classes.
	 * @param responseEndpoint
	 * @param connectionClasses
	 */
	@SafeVarargs
	public ConnectionRequest(AcceptsConnectionResponse responseEndpoint, Class<?extends Connection>... connectionClasses) {
		this.responseEndpoint = responseEndpoint;
		this.connections = new LinkedList<>();
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
	 * @return the stored object that shall receive a response.
	 */
	public AcceptsConnectionResponse getResponseEndpoint() {
		return this.responseEndpoint;
	}
	
	/**
	 * @return the iterable of connection classes.
	 */
	@SuppressWarnings("unchecked")
	public Iterable<Class<?extends Connection>> getRequestedConnections() {
		return (Iterable<Class<? extends Connection>>) this.connections.clone();
	}
}