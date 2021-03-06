package org.hucompute.service.uima.eval.connection;

import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
	void Given_ConnectionResponseWithFewResults_When_GettingConnectionList_Then_MapOfConnectionsIsReturned()
	{
		Connection a = new TestConnectionA();
		Connection b = new TestConnectionB();

		Map<Class<? extends Connection>, Connection> map = new HashMap<>();
		map.put(TestConnectionA.class, a);
		map.put(TestConnectionB.class, b);

		ConnectionResponse response = new ConnectionResponse();
		response.addConnection(a);
		response.addConnection(b);

		assertEquals(map.size(), response.getConnections().size());
		assertEquals(a, response.getConnection(TestConnectionA.class));
		assertEquals(b, response.getConnection(TestConnectionB.class));
	}
}
