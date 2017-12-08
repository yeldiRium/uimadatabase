package uimadatabase.dbtest.connection;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;

class ConnectionRequestTestCase {
	protected class TestConnectionA extends Connection {}
	protected class TestConnectionB extends Connection {}
	protected class TestConnectionC extends Connection {}
	
	@Test
	void Given_EmptyRequestObject_When_RetrievingRequestInformation_Then_EmptyIterableIsReturned() {
		ConnectionRequest request = new ConnectionRequest();
		Iterable res = request.getRequestedConnections();
		assertFalse(res.iterator().hasNext());
	}

	@Test
	void Given_RequestObjectConstructedWithParameterList_When_RetrievingRequestInformation_Then_ListOfConnectionClassesIsReturnedAsIterable() {
		List<Class> list = new LinkedList<>();
		list.add(TestConnectionA.class);
		list.add(TestConnectionC.class);
		
		ConnectionRequest request = new ConnectionRequest(
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
		
		ConnectionRequest request = new ConnectionRequest();
		request.addRequestedConnection(TestConnectionA.class);
		request.addRequestedConnection(TestConnectionB.class);
		
		assertIterableEquals(list, request.getRequestedConnections());
	}
	
	@Test
	void Given_RequestObjectConstructedWithParameterListAndAddMethod_When_RetrievingRequestInformation_Then_ListOfConnectionClassesIsReturnedAsIterable() {
		List<Class> list = new LinkedList<>();
		list.add(TestConnectionB.class);
		list.add(TestConnectionC.class);
		
		ConnectionRequest request = new ConnectionRequest(TestConnectionB.class);
		request.addRequestedConnection(TestConnectionC.class);
		
		assertIterableEquals(list, request.getRequestedConnections());
	}
}
