package org.hucompute.services.uima.eval.database.connection.implementation;

import org.hucompute.services.uima.eval.database.connection.Connection;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

/**
 * Establishes and exposes a connection to the Neo4j Server.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class Neo4jConnection extends Connection
{
	protected Driver driver;

	public Driver getDriver()
	{
		return this.driver;
	}

	@Override
	protected boolean tryToConnect()
	{
		// If the connection is already established, nothing further has to be
		// done
		if (this.driver != null)
		{
			return true;
		}

		try
		{
			String host = System.getenv("NEO4J_HOST");
			String port = System.getenv("NEO4J_PORT");
			String username = System.getenv("NEO4J_USER");
			String password = System.getenv("NEO4J_PASS");

			this.driver = GraphDatabase.driver(
					"bolt://" + host + ":" + port,
					AuthTokens.basic(username, password)
			);
			return true;
		} catch (ServiceUnavailableException e)
		{
			return false;
		}
	}

	@Override
	public void close()
	{
		this.driver.close();
	}
}
