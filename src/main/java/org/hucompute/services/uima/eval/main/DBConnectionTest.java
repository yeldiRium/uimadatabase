package org.hucompute.services.uima.eval.main;

import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.database.connection.implementation.*;
import org.hucompute.services.uima.eval.utility.logging.PlainFormatter;

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
public class DBConnectionTest
{
	protected static Logger logger = Logger.getLogger(
			DBConnectionTest.class.getName()
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
