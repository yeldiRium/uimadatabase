package uimadatabase.dbtest.connection;


import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import dbtest.connection.Connection;

class ConnectionTestCase {
	
	class TestConnection extends Connection {
		@Override
		protected boolean tryToConnect() {
			return false;
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
		assertEquals(false, connection.isEstablished());
		// Since the Connection currently sleep 500ms between connection tries
		// (see Connection.sleepTime), and we make it take four calls, 3000ms
		// should be enough.
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// IDGAF
		}
		verify(connection, times(4)).tryToConnect();
		assertEquals(true, connection.isEstablished());
	}

}
