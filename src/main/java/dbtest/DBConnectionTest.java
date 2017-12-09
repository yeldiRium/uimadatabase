package dbtest;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import dbtest.connection.Connection;
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
public class DBConnectionTest {
	public static void main(String[] args) {
		ConnectionManager connectionManager = new ConnectionManager();
		
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(ArangoDBConnection.class);
		connectionRequest.addRequestedConnection(BaseXConnection.class);
		connectionRequest.addRequestedConnection(CassandraConnection.class);
		connectionRequest.addRequestedConnection(MongoDBConnection.class);
		connectionRequest.addRequestedConnection(MySQLConnection.class);
		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		
		Future<ConnectionResponse> futureConnectionResponse
			= connectionManager.submitRequest(connectionRequest);
		
		try {
			ConnectionResponse connectionResponse = futureConnectionResponse.get();
			System.out.println("Connections Established:");
			for(Connection connection: connectionResponse.getConnections()) {
				if(connection.isEstablished()) {					
					System.out.println("Connection " + connection.getClass().getName() + " established.");
				}
			}
			
		} catch (InterruptedException | ExecutionException e) {
			System.out.println("Something went really wrong:");
			e.printStackTrace();
		}
	}
}