package org.hucompute.service.uima.eval.connection;

import org.hucompute.services.uima.eval.database.connection.Connections;
import org.hucompute.services.uima.eval.database.connection.implementation.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

public class ConnectionsTestCase
{
	@Test
	void Given_ConnectionsHelperClass_When_QueryingConnectionClassByEnum_Then_CorrectClassIsReturned()
	{
		assertSame(
				Connections.getConnectionClassForDB(Connections.DBName.ArangoDB),
				ArangoDBConnection.class
		);
		assertSame(
				Connections.getConnectionClassForDB(Connections.DBName.Cassandra),
				CassandraConnection.class
		);
		assertSame(
				Connections.getConnectionClassForDB(Connections.DBName.BaseX),
				BaseXConnection.class
		);
		assertSame(
				Connections.getConnectionClassForDB(Connections.DBName.MongoDB),
				MongoDBConnection.class
		);
		assertSame(
				Connections.getConnectionClassForDB(Connections.DBName.MySQL),
				MySQLConnection.class
		);
		assertSame(
				Connections.getConnectionClassForDB(Connections.DBName.Neo4j),
				Neo4jConnection.class
		);
	}

	@Test
	void Given_ConnectionsHelperClass_When_QueryingConnectionClassByString_Then_CorrectClassIsReturned()
	{
		assertSame(
				Connections.getConnectionClassForName(Connections.DBName.ArangoDB.toString()),
				ArangoDBConnection.class
		);
		assertSame(
				Connections.getConnectionClassForName(Connections.DBName.Cassandra.toString()),
				CassandraConnection.class
		);
		assertSame(
				Connections.getConnectionClassForName(Connections.DBName.BaseX.toString()),
				BaseXConnection.class
		);
		assertSame(
				Connections.getConnectionClassForName(Connections.DBName.MongoDB.toString()),
				MongoDBConnection.class
		);
		assertSame(
				Connections.getConnectionClassForName(Connections.DBName.MySQL.toString()),
				MySQLConnection.class
		);
		assertSame(
				Connections.getConnectionClassForName(Connections.DBName.Neo4j.toString()),
				Neo4jConnection.class
		);
	}
}
