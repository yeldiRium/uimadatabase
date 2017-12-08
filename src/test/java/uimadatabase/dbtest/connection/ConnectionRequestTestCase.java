package uimadatabase.dbtest.connection;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedList;
import java.util.List;

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
	void Given_EmptyRequestObject_When_RetrievingRequestInformation_Then_EmptyIterableIsReturned() {
		ConnectionRequest request = new ConnectionRequest(this.requestor);
		Iterable res = request.getRequestedConnections();
		assertFalse(res.iterator().hasNext());
	}

	@Test
	void Given_RequestObjectConstructedWithParameterList_When_RetrievingRequestInformation_Then_ListOfConnectionClassesIsReturnedAsIterable() {
		List<Class> list = new LinkedList<>();
		list.add(TestConnectionA.class);
		list.add(TestConnectionC.class);
		
		ConnectionRequest request = new ConnectionRequest(
				this.requestor,
				TestConnectionA.class,
				TestConnectionC.class
		);
		
		assertIterableEquals(list, request.getRequestedConnections());
	}
	
	@Test
	void Given_RequestObjectConstructedWithAddMethod_When_RetrievingRequestInformation_Then_ListOfConnectionClassesIsReturnedAsIterable() {
		List<Class> list = new LinkedList<>();
		list.add(TestConnectionA.class);
		list.add(TestConnectionB.class);
		
		ConnectionRequest request = new ConnectionRequest(this.requestor);
		request.addRequestedConnection(TestConnectionA.class);
		request.addRequestedConnection(TestConnectionB.class);
		
		assertIterableEquals(list, request.getRequestedConnections());
	}
	
	@Test
	void Given_RequestObjectConstructedWithParameterListAndAddMethod_When_RetrievingRequestInformation_Then_ListOfConnectionClassesIsReturnedAsIterable() {
		List<Class> list = new LinkedList<>();
		list.add(TestConnectionB.class);
		list.add(TestConnectionC.class);
		
		ConnectionRequest request = new ConnectionRequest(this.requestor, TestConnectionB.class);
		request.addRequestedConnection(TestConnectionC.class);
		
		assertIterableEquals(list, request.getRequestedConnections());
	}
}
