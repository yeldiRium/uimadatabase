package org.hucompute.services.uima.eval.database.connection.implementation;

import org.hucompute.services.uima.eval.database.connection.Connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ImplementationMap
{
	public static final Map<String, Class<? extends Connection>> implementationMap;

	static
	{
		Map<String, Class<? extends Connection>> tempMap = new HashMap<>();
		tempMap.put(
				"arangodb", ArangoDBConnection.class
		);
		tempMap.put(
				"basex", BaseXConnection.class
		);
		tempMap.put(
				"cassandra", CassandraConnection.class
		);
		tempMap.put(
				"mongodb", MongoDBConnection.class
		);
		tempMap.put(
				"mysql", MySQLConnection.class
		);
		tempMap.put(
				"neo4j", Neo4jConnection.class
		);
		implementationMap = Collections.unmodifiableMap(tempMap);
	}
}
