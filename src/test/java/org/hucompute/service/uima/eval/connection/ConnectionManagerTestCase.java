package org.hucompute.service.uima.eval.connection;

import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

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

	@Test
	void Given_ConnectionManager_When_SubmittingNullAsRequest_Then_ThrowsNullException()
	{
		ConnectionManager connectionManager = ConnectionManager.getInstance();
		assertThrows(
				NullPointerException.class,
				() -> connectionManager.submitRequest(null)
		);
	}

	@Tag("slow")
	@Test
	void Given_ConnectionManagerAndMockedConnection_When_SubmittingRequestForMockedConnection_Then_EstablishAndIsEstablishedWillBeCalledOnMock() throws InterruptedException, ExecutionException
	{
		ConnectionManager connectionManager = ConnectionManager.getInstance();
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(MockConnection.class);

		Future<ConnectionResponse> futureConnectionResponse = connectionManager.submitRequest(connectionRequest);

		ConnectionResponse connectionResponse = futureConnectionResponse.get();
		Collection<Connection> connections = connectionResponse.getConnections();

		assertEquals(1, connections.size(), "One Connection was requested, one should be returned.");
		MockConnection mockedConnection = (MockConnection) connectionResponse.getConnection(MockConnection.class);
		assertEquals(1, mockedConnection.establishCounter, "Establish should've been called exactly once.");
		assertEquals(true, mockedConnection.isEstablished(), "IsEstablished should be true by now.");
	}

	@Test
	void Given_ConnectionManagerAndMockedConnection_When_ClosingConnectionManager_Then_AllConnectionsWillBeClosed() throws ExecutionException, InterruptedException
	{
		ConnectionManager connectionManager = ConnectionManager.getInstance();
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(MockConnection.class);

		Future<ConnectionResponse> futureConnectionResponse = connectionManager.submitRequest(connectionRequest);
		ConnectionResponse connectionResponse = futureConnectionResponse.get();
		MockConnection mockedConnection = (MockConnection) connectionResponse.getConnection(MockConnection.class);

		connectionManager.close();
		assertTrue(mockedConnection.wasClosed);
	}
}
