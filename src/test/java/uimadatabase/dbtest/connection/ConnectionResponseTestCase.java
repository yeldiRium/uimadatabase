package uimadatabase.dbtest.connection;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dbtest.connection.Connection;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.ConnectionResponseAlreadyFinishedException;

class ConnectionResponseTestCase {
	protected class TestConnectionA extends Connection {}
	protected class TestConnectionB extends Connection {}

	@Test
	void Given_EmptyConnectionResponse_When_AskingIsFinished_Then_ReturnsFalse() {
		ConnectionResponse response = new ConnectionResponse();
		assertFalse(response.isFinished());
	}
	
	@Test
	void Given_FinishedConnectionResponse_When_AskingIsFinished_Then_ReturnsTrue() {
		ConnectionResponse response = new ConnectionResponse();
		response.finish();
		assertTrue(response.isFinished());
	}
	
	@Test
	void Given_FinishedConnectionResponse_When_TryingToAddConnection_Then_ExceptionIsThrown() {
		Connection a = new TestConnectionA();
		ConnectionResponse response = new ConnectionResponse();
		response.finish();
		assertThrows(
			ConnectionResponseAlreadyFinishedException.class,
			() -> response.addConnection(a)
		);
	}
	
	@Test
	void Given_EmptyConnectionResponse_When_GettingConnectionList_Then_EmptyIterableIsReturned() {
		ConnectionResponse response = new ConnectionResponse();
		assertFalse(response.getConnections().iterator().hasNext());
	}

	@Test
	void Given_ConnectionResponseWithFewResults_When_GettingConnectionList_Then_IterableOfConnectionsIsReturned() {
		Connection a = new TestConnectionA();
		Connection b = new TestConnectionB();
		
		List<Connection> list = new LinkedList<>();
		list.add(a);
		list.add(b);
		
		ConnectionResponse response = new ConnectionResponse();
		response.addConnection(a);
		response.addConnection(b);
		
		assertIterableEquals(list, response.getConnections());
	}
}
