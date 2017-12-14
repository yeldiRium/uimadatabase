package uimadatabase.dbtest.connection;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;

public class ConnectionRequestTestCase {
	protected class TestConnectionA extends Connection {
		@Override
		protected boolean tryToConnect() {
			// TODO Auto-generated method stub
			return false;
		}
	}
	protected class TestConnectionB extends Connection {
		@Override
		protected boolean tryToConnect() {
			// TODO Auto-generated method stub
			return false;
		}
	}
	protected class TestConnectionC extends Connection {
		@Override
		protected boolean tryToConnect() {
			// TODO Auto-generated method stub
			return false;
		}
	}
	
	@Test
	void Given_EmptyRequestObject_When_RetrievingRequestInformation_Then_EmptySetIsReturned() {
		ConnectionRequest request = new ConnectionRequest();
		assertEquals(0, request.getRequestedConnections().size());
	}

	@Test
	void Given_RequestObjectConstructedWithParameterList_When_RetrievingRequestInformation_Then_SetOfConnectionClassesIsReturned() {
		Set<Class> set = new HashSet<>();
		set.add(TestConnectionA.class);
		set.add(TestConnectionC.class);
		
		ConnectionRequest request = new ConnectionRequest(
				TestConnectionA.class,
				TestConnectionC.class
		);
		
		assertEquals(set.size(), request.getRequestedConnections().size());
		assertTrue(set.containsAll(request.getRequestedConnections()));
	}
	
	@Test
	void Given_RequestObjectConstructedWithAddMethod_When_RetrievingRequestInformation_Then_SetOfConnectionClassesIsReturned() {
		Set<Class> set = new HashSet<>();
		set.add(TestConnectionA.class);
		set.add(TestConnectionB.class);
		
		ConnectionRequest request = new ConnectionRequest();
		request.addRequestedConnection(TestConnectionA.class);
		request.addRequestedConnection(TestConnectionB.class);

		assertEquals(set.size(), request.getRequestedConnections().size());
		assertTrue(set.containsAll(request.getRequestedConnections()));
	}
	
	@Test
	void Given_RequestObjectConstructedWithParameterListAndAddMethod_When_RetrievingRequestInformation_Then_SetOfConnectionClassesIsReturned() {
		Set<Class> set = new HashSet<>();
		set.add(TestConnectionB.class);
		set.add(TestConnectionC.class);
		
		ConnectionRequest request = new ConnectionRequest(TestConnectionB.class);
		request.addRequestedConnection(TestConnectionC.class);

		assertEquals(set.size(), request.getRequestedConnections().size());
		assertTrue(set.containsAll(request.getRequestedConnections()));
	}
}
