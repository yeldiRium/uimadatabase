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
	
	public ConnectionRequest(AcceptsConnectionResponse responseEndpoint) {
		this.responseEndpoint = responseEndpoint;
		this.connections = new LinkedList<>(); 
	}
	
	@SafeVarargs
	public ConnectionRequest(AcceptsConnectionResponse responseEndpoint, Class<?extends Connection>... connectionClasses) {
		this.responseEndpoint = responseEndpoint;
		this.connections = new LinkedList<>();
		for(Class<?extends Connection> connectionClass: connectionClasses) {
			this.connections.add(connectionClass);
		}
	}
	
	public void addRequestedConnection(Class<?extends Connection> connectionClass) {
		this.connections.add(connectionClass);
	}
	
	public AcceptsConnectionResponse getResponseEndpoint() {
		return this.responseEndpoint;
	}
	
	@SuppressWarnings("unchecked")
	public Iterable<Class<?extends Connection>> getRequestedConnections() {
		return (Iterable<Class<? extends Connection>>) this.connections.clone();
	}
}
