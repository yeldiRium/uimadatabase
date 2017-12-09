package uimadatabase.dbtest.connection;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import static org.mockito.Mockito.verify;

import dbtest.connection.AcceptsConnectionResponse;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionRequestAlreadySubmittedException;
import dbtest.connection.ConnectionResponse;

class ConnectionManagerTestCase {
	/**
	 * Will count method calls for testing purposes.
	 */
	public static class MockConnection extends Connection {
		public int establishCounter = 0;
		public int isEstablishedCounter = 0;
		
		public MockConnection() {
			
		}
		
		public void establish() {
			this.establishCounter++;;
		}
		
		public boolean isEstablished() {
			this.isEstablishedCounter++;
			latch.countDown();
			// Let's ask 5 times, then returns true.
			return this.isEstablishedCounter >= 5;
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
	
	protected static CountDownLatch latch;
	
	@Tag("slow")
	@Test
	void Given_ConnectionManagerAndMockedConnection_When_SubmittingRequestForMockedConnection_Then_EstablishAndIsEstablishedWillBeCalledOnMock() {
		ConnectionManagerTestCase.latch = new CountDownLatch(5);
		AcceptsConnectionResponse mockedResponseEndpoint = Mockito.mock(AcceptsConnectionResponse.class);
		ConnectionManager connectionManager = new ConnectionManager();
		ConnectionRequest connectionRequest = new ConnectionRequest(mockedResponseEndpoint);
		connectionRequest.addRequestedConnection(MockConnection.class);
		
		connectionManager.submitRequest(connectionRequest);
		try {
			latch.await();
		} catch (InterruptedException e) {
			// IDGAF
		}
		
		// Test, that the mockedResponseEndpoint actually got a response
		this.captor = ArgumentCaptor.forClass(ConnectionResponse.class);
		verify(mockedResponseEndpoint).acceptResponse(this.captor.capture());
		ConnectionResponse connectionResponse = captor.getValue();
		Set<Connection> connections = connectionResponse.getConnections();
		
		assertEquals(1, connections.size(), "One Connection was requested, one should be returned.");
		MockConnection mockedConnection = (MockConnection) connections.toArray()[0];
		assertEquals(1, mockedConnection.establishCounter, "Establish should've been called exactly once.");
		assertEquals(5, mockedConnection.isEstablishedCounter, "IsEstablished should've been called exactly five times.");
	}
	
	@Test
	void Given_ConnectionManager_When_SubmittingConnectionMultipleTimes_Then_ExceptionIsThrown() {
		AcceptsConnectionResponse mockedResponseEndpoint = Mockito.mock(AcceptsConnectionResponse.class);
		ConnectionManager connectionManager = new ConnectionManager();
		ConnectionRequest connectionRequest = new ConnectionRequest(mockedResponseEndpoint);
		connectionRequest.addRequestedConnection(NoConnection.class);
		connectionManager.submitRequest(connectionRequest);
		assertThrows(
			ConnectionRequestAlreadySubmittedException.class,
			() -> connectionManager.submitRequest(connectionRequest)
		);
	}
}
