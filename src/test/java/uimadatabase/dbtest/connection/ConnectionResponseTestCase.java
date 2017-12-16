package uimadatabase.dbtest.connection;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;

import dbtest.connection.Connection;
import dbtest.connection.ConnectionResponse;

public class ConnectionResponseTestCase {
	protected class TestConnectionA extends Connection {
		@Override
		protected boolean tryToConnect() {
			// TODO Auto-generated method stub
			return false;
		}}
	protected class TestConnectionB extends Connection {
		@Override
		protected boolean tryToConnect() {
			// TODO Auto-generated method stub
			return false;
		}}
	
	@Test
	void Given_EmptyConnectionResponse_When_GettingConnectionList_Then_EmptySetIsReturned() {
		ConnectionResponse response = new ConnectionResponse();
		assertEquals(0, response.getConnections().size());
	}

	@Test
	void Given_ConnectionResponseWithFewResults_When_GettingConnectionList_Then_SetOfConnectionsIsReturned() {
		Connection a = new TestConnectionA();
		Connection b = new TestConnectionB();
		
		Set<Connection> set = new HashSet<>();
		set.add(a);
		set.add(b);
		
		ConnectionResponse response = new ConnectionResponse();
		response.addConnection(a);
		response.addConnection(b);
		
		assertEquals(set.size(), response.getConnections().size());
		assertTrue(set.containsAll(response.getConnections()));
	}
}
