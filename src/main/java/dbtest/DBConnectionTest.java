package dbtest;

import dbtest.connection.AcceptsConnectionResponse;
import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.ArangoDBConnection;
import dbtest.connection.implementation.BaseXConnection;
import dbtest.connection.implementation.CassandraConnection;
import dbtest.connection.implementation.MongoDBConnection;
import dbtest.connection.implementation.MySQLConnection;
import dbtest.connection.implementation.Neo4jConnection;

/**
 * Requests all Connections from the ConnectionManager.
 * 
 * This is meant to test if the docker setup works correctly.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class DBConnectionTest implements AcceptsConnectionResponse {
	public static void main(String[] args) {
		ConnectionManager connectionManager = new ConnectionManager();
		DBConnectionTest main = new DBConnectionTest();
		ConnectionRequest connectionRequest = new ConnectionRequest(main);
		connectionRequest.addRequestedConnection(ArangoDBConnection.class);
		connectionRequest.addRequestedConnection(BaseXConnection.class);
		connectionRequest.addRequestedConnection(CassandraConnection.class);
		connectionRequest.addRequestedConnection(MongoDBConnection.class);
		connectionRequest.addRequestedConnection(MySQLConnection.class);
		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		connectionManager.submitRequest(connectionRequest);
	}

	@Override
	public void acceptResponse(ConnectionResponse response) {
		System.out.println("Response received. Connection to all databases established.");
	}
}
