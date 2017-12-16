package uimadatabase.dbtest.connection;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import dbtest.connection.Connection;

public class ConnectionTestCase {
	
	class TestConnection extends Connection {
		@Override
		protected boolean tryToConnect() {
			return false;
		}

		@Override
		public void close()
		{

		}

	}

	@Test
	void Given_ConnectionWithMockedTryToConnectMethod_When_CallingEstablish_Then_CallsTryToConnectUntilItReturnsTrue() {
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
