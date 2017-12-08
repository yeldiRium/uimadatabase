package uimadatabase.dbtest.connection;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import dbtest.connection.AcceptsConnectionResponse;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;

class ConnectionRequestTestCase {
	protected class TestConnectionA extends Connection {}
	protected class TestConnectionB extends Connection {}
	protected class TestConnectionC extends Connection {}
	
	protected AcceptsConnectionResponse requestor;
	
	@BeforeClass
	void BeforeClass() {
		this.requestor = Mockito.mock(AcceptsConnectionResponse.class);
	}
	
	@Test
	void Given_EmptyRequestObject_When_RetrievingRequestor_Then_GivenRequestorIsReturned() {
		ConnectionRequest request = new ConnectionRequest(this.requestor);
		assertSame(this.requestor, request.getResponseEndpoint());
	}
	
	@Test
	void Given_EmptyRequestObject_When_RetrievingRequestInformation_Then_EmptySetIsReturned() {
		ConnectionRequest request = new ConnectionRequest(this.requestor);
		assertEquals(0, request.getRequestedConnections().size());
	}

	@Test
	void Given_RequestObjectConstructedWithParameterList_When_RetrievingRequestInformation_Then_SetOfConnectionClassesIsReturned() {
		Set<Class> set = new HashSet<>();
		set.add(TestConnectionA.class);
		set.add(TestConnectionC.class);
		
		ConnectionRequest request = new ConnectionRequest(
				this.requestor,
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
		
		ConnectionRequest request = new ConnectionRequest(this.requestor);
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
		
		ConnectionRequest request = new ConnectionRequest(this.requestor, TestConnectionB.class);
		request.addRequestedConnection(TestConnectionC.class);

		assertEquals(set.size(), request.getRequestedConnections().size());
		assertTrue(set.containsAll(request.getRequestedConnections()));
	}
}
