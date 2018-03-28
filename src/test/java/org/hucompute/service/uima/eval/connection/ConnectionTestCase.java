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
	}

	@Test
	void Given_ConnectionWithMockedTryToConnectMethod_When_CallingEstablish_Then_CallsTryToConnectUntilItReturnsTrue()
	{
		TestConnection connection = Mockito.mock(TestConnection.class);
		doCallRealMethod().when(connection).establish();
		doCallRealMethod().when(connection).isEstablished();
		// Test that tryToConnect is called four times, the fourth time succeeding.
		when(connection.tryToConnect()).thenReturn(false, false, false, true);

		connection.establish();

		verify(connection, times(4)).tryToConnect();
		assertEquals(true, connection.isEstablished());
	}
}
