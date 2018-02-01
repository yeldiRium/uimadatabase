package dbtest;

import dbtest.connection.Connection;
import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.*;
import dbtest.logging.PlainFormatter;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.*;

/**
 * Requests all Connections from the ConnectionManager.
 * <p>
 * This is meant to test if the docker setup works correctly.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class DBConnectionTestMain
{
	protected static Logger logger = Logger.getLogger(
			DBConnectionTestMain.class.getName()
	);

	public static void main(String[] args)
	{
		Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(Level.ALL);
		Formatter plainFormatter = new PlainFormatter();
		for(Handler h : rootLogger.getHandlers())
		{
			h.setFormatter(plainFormatter);
			h.setLevel(Level.ALL);
		}

		ConnectionManager connectionManager = ConnectionManager.getInstance();
		logger.info("blub");

		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(ArangoDBConnection.class);
		connectionRequest.addRequestedConnection(BaseXConnection.class);
		connectionRequest.addRequestedConnection(CassandraConnection.class);
		connectionRequest.addRequestedConnection(MongoDBConnection.class);
		connectionRequest.addRequestedConnection(MySQLConnection.class);
		connectionRequest.addRequestedConnection(Neo4jConnection.class);

		logger.info("blub2");

		Future<ConnectionResponse> futureConnectionResponse
				= connectionManager.submitRequest(connectionRequest);

		logger.info("blub3");

		try
		{
			ConnectionResponse connectionResponse = futureConnectionResponse.get();
			logger.info("Connections Established:");
			for (Connection connection : connectionResponse.getConnections())
			{
				if (connection.isEstablished())
				{
					logger.info("Connection " + connection.getClass().getName() + " established.");
				}
			}

		} catch (InterruptedException | ExecutionException e)
		{
			logger.info("Something went really wrong:");
			e.printStackTrace();
		}
	}
}
