package uimadatabase.dbtest.connection;

import dbtest.connection.Connection;
import dbtest.connection.ConnectionResponse;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectionResponseTestCase
{
	protected class TestConnectionA extends Connection
	{
		@Override
		protected boolean tryToConnect()
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void close()
		{

		}
	}

	protected class TestConnectionB extends Connection
	{
		@Override
		protected boolean tryToConnect()
		{
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void close()
		{

		}
	}

	@Test
	void Given_EmptyConnectionResponse_When_GettingConnectionList_Then_EmptySetIsReturned()
	{
		ConnectionResponse response = new ConnectionResponse();
		assertEquals(0, response.getConnections().size());
	}

	@Test
	void Given_ConnectionResponseWithFewResults_When_GettingConnectionList_Then_SetOfConnectionsIsReturned()
	{
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
