package dbtest.connection;

import dbtest.connection.implementation.*;

public class Connections
{
	public enum DBName
	{
		ArangoDB, BaseX, Cassandra, MongoDB, MySQL, Neo4j
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
			case Cassandra:
				return CassandraConnection.class;
			case MongoDB:
				return MongoDBConnection.class;
			case MySQL:
				return MySQLConnection.class;
			case Neo4j:
				return Neo4jConnection.class;
			default:
				return null;
		}
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
}
