package dbtest.connection;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * The ConnectionManager creates Connection objects based on ConnectionRequests
 * and returns them in ConnectionResponse objects.
 * This abstracts the actual Connection process away and allows the recipients
 * of the Future<ConnectionResponse> objects to wait until all required Connec-
 * tions are established.
 *
 * In case the ConnectionManager or its Connections can not be injected some-
 * where, it is used as a Singleton.
 */
public class ConnectionManager
{
	private static final class InstanceHolder
	{
		static ConnectionManager INSTANCE = new ConnectionManager();
	}

	protected static final Logger LOGGER = Logger.getLogger(ConnectionManager.class.getName());
	protected static final int threadCount = 10;

	protected Map<Class<? extends Connection>, Future<Connection>> connections;
	protected ExecutorService executor;

	private ConnectionManager()
	{
		this.connections = new HashMap<>();
		this.executor = Executors.newFixedThreadPool(ConnectionManager.threadCount);
		LOGGER.info("ConnectionManager initialized.");
	}

	public static ConnectionManager getInstance()
	{
		if (InstanceHolder.INSTANCE == null)
		{
			InstanceHolder.INSTANCE = new ConnectionManager();
		}
		return InstanceHolder.INSTANCE;
	}

	/**
	 * Checks all connections the given Request wants.
	 * If a connection is not registered yet, a future for it will be created.
	 * If a connection is already established, it will be added to a response object.
	 * If all connections are established, the Response is finished and the responseEndpoint called.
	 *
	 * @param connectionRequest
	 */
	public Future<ConnectionResponse> submitRequest(ConnectionRequest connectionRequest)
	{
		Objects.requireNonNull(connectionRequest);

		LOGGER.finer("Request received. Processing in new Thread...");
		return this.executor.submit(() -> {
			LOGGER.finer("Processing Thread started.");
			ConnectionResponse connectionResponse = new ConnectionResponse();

			for (Class<? extends Connection> cls : connectionRequest.getRequestedConnections())
			{
				Future<Connection> connectionFuture = connections.get(cls);
				if (connectionFuture == null)
				{
					LOGGER.finer("Connection to " + cls.getName() + " not found. Starting one in new Thread...");
					connectionFuture = executor.submit(() -> {
						LOGGER.finer("Connection Thread started.");
						try
						{
							Connection connection = cls.getConstructor().newInstance();
							connection.establish();
							return connection;
						} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
								| InvocationTargetException | NoSuchMethodException | SecurityException e)
						{
							// will never happen, since Connection has a fitting Constructor and all classes used
							// here extend Connection.
							LOGGER.severe("Exception occurred during establishment of Connection to " + cls.getName());
							e.printStackTrace();
							return null;
						}
					});
					connections.put(cls, connectionFuture);
				}
			}
			// get the Futures' responses in a second loop, so all of them are started before
			// the first blocking execution.
			for (Class<? extends Connection> cls : connectionRequest.getRequestedConnections())
			{
				Future<Connection> connectionFuture = connections.get(cls);
				connectionResponse.addConnection(connectionFuture.get());
			}
			// Call endpoint with Response object.
			LOGGER.info("Response built. Returning...");
			return connectionResponse;
		});
	}

	/**
	 * Closes all connections that are open.
	 * Cancels all futures, which have not yet been completed.
	 */
	public void close()
	{
		for (Future<Connection> futureConnection : this.connections.values())
		{
			if (futureConnection.isDone())
			{
				try
				{
					futureConnection.get().close();
				} catch (InterruptedException | ExecutionException e)
				{
					e.printStackTrace();
				}
			} else
			{
				futureConnection.cancel(true);
			}
		}

		LOGGER.info("Shutting down connection thread pool...");
		this.executor.shutdown();
		try {
			if (!this.executor.awaitTermination(1, TimeUnit.SECONDS))
			{
				this.executor.shutdownNow();
				if (!this.executor.awaitTermination(1, TimeUnit.SECONDS))
				{
					LOGGER.severe("Connection thread pool refuses to shut down!");
				}
			}
		} catch (InterruptedException e)
		{
			this.executor.shutdownNow();
			Thread.currentThread().interrupt();
		}

		// Set INSTANCE to null so that another call to getInstance will create
		// everything anew
		InstanceHolder.INSTANCE = null;
	}
}
