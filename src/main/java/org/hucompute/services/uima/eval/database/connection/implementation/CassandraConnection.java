package org.hucompute.services.uima.eval.database.connection.implementation;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.hucompute.services.uima.eval.database.connection.Connection;

/**
 * Establishes and exposes a connection to the Cassandra Server.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class CassandraConnection extends Connection
{
	protected Cluster cluster;
	protected Session session;

	public Cluster getCluster()
	{
		return cluster;
	}

	public Session getSession()
	{
		return this.session;
	}

	@Override
	protected boolean tryToConnect()
	{
		// If the connection is already established, nothing further has to be
		// done
		if (this.session != null)
		{
			return true;
		}

		try
		{
			// Create cluster with credentials from environment variables
			this.cluster = Cluster.builder()
					.addContactPoint(System.getenv("CASSANDRA_HOST"))
					.withPort(
							Integer.parseInt(System.getenv("CASSANDRA_PORT"))
					)
					.withCredentials(
							System.getenv("CASSANDRA_USER"),
							System.getenv("CASSANDRA_PASS")
					)
					.build();
			// Connect to server
			this.session = cluster.connect();
			return true;
		} catch (NoHostAvailableException | AuthenticationException |
				IllegalStateException e)
		{
			return false;
		}
	}

	@Override
	public void close()
	{
		this.session.close();
		this.cluster.close();
	}
}
