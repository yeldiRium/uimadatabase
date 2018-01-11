package uimadatabase.dbtest.queryHandler;

import dbtest.connection.Connection;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.implementation.Neo4jQueryHandler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryHandlerInterfaceTestCase
{
	@Test
	void Given_Neo4jConnectionCastToConnection_When_RequestingQueryHandlerViaFactoryMethod_Then_CorrectQueryHandlerIsReturned()
	{
		Connection connection = (Connection) new Neo4jConnection();
		QueryHandlerInterface queryHandler =
				QueryHandlerInterface.createQueryHandlerForConnection(connection);
		assertTrue(queryHandler instanceof Neo4jQueryHandler);
	}
}
