package dbtest.connection;

import java.util.LinkedList;

public class ConnectionRequest {
	private LinkedList<Class<?extends Connection>> connections;
	
	public ConnectionRequest() {
		this.connections = new LinkedList<>(); 
	}
	
	@SafeVarargs
	public ConnectionRequest(Class<?extends Connection>... connectionClasses) {
		this.connections = new LinkedList<>();
		for(Class<?extends Connection> connectionClass: connectionClasses) {
			this.connections.add(connectionClass);
		}
	}
	
	public void addRequestedConnection(Class<?extends Connection> connectionClass) {
		this.connections.add(connectionClass);
	}
	
	@SuppressWarnings("unchecked")
	public Iterable<Class<?extends Connection>> getRequestedConnections() {
		return (Iterable<Class<? extends Connection>>) this.connections.clone();
	}
}
