package org.hucompute.service.uima.eval.connection;

import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.abstraction.QueryHandlerInterface;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

public class ConnectionTestCase
{
	class TestConnection extends Connection
	{
		public QueryHandlerInterface injectedQueryHandler;

		@Override
		protected boolean tryToConnect()
		{
			return true;
		}

		@Override
		public void close()
		{

		}

		@Override
		protected void createQueryHandler()
		{
			this.queryHandler = this.injectedQueryHandler;
		}
	}

	@Test
	void Given_ConnectionWithMockedTryToConnectMethod_When_CallingEstablish_Then_CallsTryToConnectUntilItReturnsTrue()
	{
		TestConnection connection = Mockito.mock(TestConnection.class);
		doCallRealMethod().when(connection).establish();
		doCallRealMethod().when(connection).isEstablished();
		doCallRealMethod().when(connection).createQueryHandler();
		// Test that tryToConnect is called four times, the fourth time succeeding.
		when(connection.tryToConnect()).thenReturn(false, false, false, true);
		when(connection.getQueryHandler()).thenReturn(Mockito.mock(QueryHandlerInterface.class));

		connection.establish();

		verify(connection, times(4)).tryToConnect();
		assertEquals(true, connection.isEstablished());
	}

	@Test
	void Given_Connection_When_Established_Then_QueryHandlerShouldBeGettable()
	{
		TestConnection connection = new TestConnection();
		connection.injectedQueryHandler = Mockito.mock(QueryHandlerInterface.class);
		connection.establish();

		assertNotNull(connection.getQueryHandler());
	}
}
