package dbtest.connection.implementation;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import dbtest.connection.Connection;

/**
 * Establishes and exposes a connection to the MongoDB Server.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class MongoDBConnection extends Connection
{
	protected MongoClient client;

	public MongoClient getClient()
	{
		return this.client;
	}

	@Override
	protected boolean tryToConnect()
	{
		// If the connection is already established, nothing further has to be
		// done
		if (this.client != null)
		{
			return true;
		}

		try
		{
			this.client = new MongoClient(
					System.getenv("MONGODB_HOST"),
					Integer.parseInt(System.getenv("MONGODB_PORT"))
			);
			return true;
		} catch (MongoException e)
		{
			return false;
		}
	}

	@Override
	protected void createQueryHandler()
	{
		// TODO: create QueryHandler, once it is implemented
	}

	@Override
	public void close()
	{
		this.client.close();
	}

}
