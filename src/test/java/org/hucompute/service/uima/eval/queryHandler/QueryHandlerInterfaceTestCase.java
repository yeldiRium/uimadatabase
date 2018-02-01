package org.hucompute.service.uima.eval.queryHandler;

import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.implementation.Neo4jConnection;
import org.hucompute.services.uima.eval.database.abstraction.QueryHandlerInterface;
import org.hucompute.services.uima.eval.database.abstraction.implementation.Neo4jQueryHandler;
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
