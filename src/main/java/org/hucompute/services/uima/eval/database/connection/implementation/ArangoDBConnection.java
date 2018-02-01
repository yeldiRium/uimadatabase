package org.hucompute.services.uima.eval.database.connection.implementation;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import org.hucompute.services.uima.eval.database.connection.Connection;

/**
 * Establishes and exposes a connection to the ArangoDB Server.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ArangoDBConnection extends Connection
{
	protected ArangoDB arangoDB;
	protected boolean connected = false;

	public ArangoDB getArangoDB()
	{
		return this.arangoDB;
	}

	@Override
	protected boolean tryToConnect()
	{
		// If the connection is already established, nothing further has to be
		// done has to check connection, since the ArangoDB object can be crea-
		// ted while the actual connection when calling getVersion fails.
		if (this.connected)
		{
			return true;
		}

		try
		{
			// Connect to ArangoDB with credentials from environment variables
			this.arangoDB = new ArangoDB.Builder()
					.host(
							System.getenv("ARANGODB_HOST"),
							Integer.parseInt(System.getenv("ARANGODB_PORT"))
					)
					.user(System.getenv("ARANGODB_USER"))
					.password(System.getenv("ARANGODB_PASS"))
					.build();
			// Request version to check, if the connection was successful.
			// Without executing anything, the connection is not actually estab-
			// lished. If the server is not reachable, this will log a
			// ConnectException, which is handled by arango and can't be pre-
			// vented. So that will always pollute the logs.
			arangoDB.getVersion();
			this.connected = true;
			return true;
		} catch (ArangoDBException e)
		{
			return false;
		}
	}

	@Override
	public void close()
	{
		this.arangoDB.shutdown();
	}
}
