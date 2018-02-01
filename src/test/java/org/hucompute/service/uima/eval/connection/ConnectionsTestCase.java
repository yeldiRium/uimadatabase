package org.hucompute.service.uima.eval.connection;

import org.hucompute.services.uima.eval.database.connection.Connections;
import org.hucompute.services.uima.eval.database.connection.implementation.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectionsTestCase
{
	@Test
	void Given_ConnectionsHelperClass_When_QueryingConnectionClassByEnum_Then_CorrectClassIsReturned()
	{
		assertTrue(
				Connections.getConnectionClassForDB(Connections.DBName.ArangoDB)
				== ArangoDBConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForDB(Connections.DBName.Cassandra)
				== CassandraConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForDB(Connections.DBName.BaseX)
				== BaseXConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForDB(Connections.DBName.MongoDB)
				== MongoDBConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForDB(Connections.DBName.MySQL)
				== MySQLConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForDB(Connections.DBName.Neo4j)
				== Neo4jConnection.class
		);
	}

	@Test
	void Given_ConnectionsHelperClass_When_QueryingConnectionClassByString_Then_CorrectClassIsReturned()
	{
		assertTrue(
				Connections.getConnectionClassForName(Connections.DBName.ArangoDB.toString())
				== ArangoDBConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForName(Connections.DBName.Cassandra.toString())
				== CassandraConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForName(Connections.DBName.BaseX.toString())
				== BaseXConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForName(Connections.DBName.MongoDB.toString())
				== MongoDBConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForName(Connections.DBName.MySQL.toString())
				== MySQLConnection.class
		);
		assertTrue(
				Connections.getConnectionClassForName(Connections.DBName.Neo4j.toString())
				== Neo4jConnection.class
		);
	}
}
