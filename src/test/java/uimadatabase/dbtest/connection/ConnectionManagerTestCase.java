package uimadatabase.dbtest.connection;

import dbtest.connection.Connection;
import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectionManagerTestCase
{
	/**
	 * Will count method calls for testing purposes.
	 */
	public static class MockConnection extends Connection
	{
		public int establishCounter = 0;
		public boolean wasClosed = false;

		public MockConnection()
		{

		}

		@Override
		public void establish()
		{
			this.establishCounter++;
		}

		@Override
		public void close()
		{
			this.wasClosed = true;
		}

		@Override
		public boolean isEstablished()
		{
			return this.establishCounter >= 1;
		}

		@Override
		protected boolean tryToConnect()
		{
			return false;
		}
	}

	/**
	 * Will never be established.
	 */
	public static class NoConnection extends Connection
	{
		public NoConnection()
		{

		}

		@Override
		protected boolean tryToConnect()
		{
			return false;
		}

		@Override
		public void close()
		{

		}
	}

	protected ArgumentCaptor<ConnectionResponse> captor;

	@Tag("slow")
	@Test
	void Given_ConnectionManagerAndMockedConnection_When_SubmittingRequestForMockedConnection_Then_EstablishAndIsEstablishedWillBeCalledOnMock() throws InterruptedException, ExecutionException
	{
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

	@Test
	void Given_ConnectionManagerAndMockedConnection_When_ClosingConnectionManager_Then_AllConnectionsWillBeClosed() throws ExecutionException, InterruptedException
	{
		ConnectionManager connectionManager = new ConnectionManager();
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(MockConnection.class);

		Future<ConnectionResponse> futureConnectionResponse = connectionManager.submitRequest(connectionRequest);
		ConnectionResponse connectionResponse = futureConnectionResponse.get();
		MockConnection mockedConnection = (MockConnection) connectionResponse.getConnections().toArray()[0];

		connectionManager.close();
		assertTrue(mockedConnection.wasClosed);
	}
}
