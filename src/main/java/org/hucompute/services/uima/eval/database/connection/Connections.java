package org.hucompute.services.uima.eval.database.connection;

import org.hucompute.services.uima.eval.database.connection.implementation.*;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class Connections
{
	public enum DBName
	{
		ArangoDB, BaseX, Blazegraph, Cassandra, MongoDB, MySQL, Neo4j, Solr
	}

	public static Class<? extends Connection> getConnectionClassForDB(
			DBName dbName
	)
	{
		switch (dbName)
		{
			case ArangoDB:
				return ArangoDBConnection.class;
			case BaseX:
				return BaseXConnection.class;
			case Blazegraph:
				return BlazegraphConnection.class;
			case Cassandra:
				return CassandraConnection.class;
			case MongoDB:
				return MongoDBConnection.class;
			case MySQL:
				return MySQLConnection.class;
			case Neo4j:
				return Neo4jConnection.class;
			case Solr:
				return SolrConnection.class;
			default:
				return null;
		}
	}

	public static DBName getIdentifierForConnectionClass(
			Class<? extends Connection> connectionClass
	)
	{
		DBName identifier = null;
		if (connectionClass == ArangoDBConnection.class)
		{
			identifier = DBName.ArangoDB;
		} else if (connectionClass == BaseXConnection.class)
		{
			identifier = DBName.BaseX;
		} else if (connectionClass == BlazegraphConnection.class)
		{
			identifier = DBName.Blazegraph;
		} else if (connectionClass == CassandraConnection.class)
		{
			identifier = DBName.Cassandra;
		} else if (connectionClass == MongoDBConnection.class)
		{
			identifier = DBName.MongoDB;
		} else if (connectionClass == MySQLConnection.class)
		{
			identifier = DBName.MySQL;
		} else if (connectionClass == Neo4jConnection.class)
		{
			identifier = DBName.Neo4j;
		} else if (connectionClass == SolrConnection.class)
		{
			identifier = DBName.Solr;
		}
		return identifier;
	}

	/**
	 * @param dbName DbName expected to match DBName.toString()
	 * @return The matching connection class.
	 */
	public static Class<? extends Connection> getConnectionClassForName(
			String dbName
	)
	{
		DBName actualDBName = DBName.valueOf(dbName);
		return getConnectionClassForDB(actualDBName);
	}

	/**
	 * @return a set of all valid and retrievable database names.
	 */
	public static Set<String> names()
	{
		return Arrays.stream(Connections.DBName.values())
				.map(Enum::name)
				.collect(Collectors.toSet());
	}
}
