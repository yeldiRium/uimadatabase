package uimadatabase.dbtest.connection;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import dbtest.connection.Connection;
import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;

class ConnectionManagerTestCase {
	/**
	 * Will count method calls for testing purposes.
	 */
	public static class MockConnection extends Connection {
		public int establishCounter = 0;
		
		public MockConnection() {
			
		}
		
		public void establish() {
			this.establishCounter++;
		}
		
		public boolean isEstablished() {
			return this.establishCounter >= 1;
		}

		@Override
		protected boolean tryToConnect() {
			return false;
		}
	}
	
	/**
	 * Will never be established.
	 */
	public static class NoConnection extends Connection {
		public NoConnection() {
			
		}
		@Override
		protected boolean tryToConnect() {
			return false;
		}
	}
	
	protected ArgumentCaptor<ConnectionResponse> captor;
	
	@Tag("slow")
	@Test
	void Given_ConnectionManagerAndMockedConnection_When_SubmittingRequestForMockedConnection_Then_EstablishAndIsEstablishedWillBeCalledOnMock() throws InterruptedException, ExecutionException {
		ConnectionManager connectionManager = new ConnectionManager();
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(MockConnection.class);
		
		Future<ConnectionResponse> futureConnectionResponse = connectionManager.submitRequest(connectionRequest);

		ConnectionResponse connectionResponse = futureConnectionResponse.get();
		Set<Connection> connections = connectionResponse.getConnections();
		
		assertEquals(1, connections.size(), "One Connection was requested, one should be returned.");
		MockConnection mockedConnection = (MockConnection) connections.toArray()[0];
		assertEquals(1, mockedConnection.establishCounter, "Establish should've been called exactly once.");
		assertEquals(true, mockedConnection.isEstablished(), "IsEstablished should be true by now.");
	}
}
