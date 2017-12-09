package dbtest.connection;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import dbtest.connection.exception.ConnectionRequestAlreadySubmittedException;

public class ConnectionManager {
	protected static final Logger LOGGER = Logger.getLogger(ConnectionManager.class.getName());
	protected static final int threadCount = 10;
	
	protected Map<Class<?extends Connection>, Future<Connection>> connections;
	protected ExecutorService executor;
	
	public ConnectionManager() {
		this.connections = new HashMap<>();
		this.executor = Executors.newFixedThreadPool(ConnectionManager.threadCount);
		LOGGER.info("ConnectionManager initialized.");
	}

	/**
	 * Checks all connections the given Request wants.
	 * If a connection is not registered yet, a future for it will be created.
	 * If a connection is already established, it will be added to a response object.
	 * If all connections are established, the Response is finished and the responseEndpoint called.
	 * @param connectionRequest
	 */
	public Future<ConnectionResponse> submitRequest(ConnectionRequest connectionRequest) {
		LOGGER.finer("Request received. Processing in new Thread...");
		return this.executor.submit(new Callable<ConnectionResponse>() {
			@Override
			public ConnectionResponse call() throws InterruptedException, ExecutionException {
				LOGGER.finer("Processing Thread started.");
				ConnectionResponse connectionResponse = new ConnectionResponse();
				
				for(Class<?extends Connection> cls: connectionRequest.getRequestedConnections()) {
					Future<Connection> connectionFuture = connections.get(cls);
					if(connectionFuture == null) {
						LOGGER.finer("Connection to " + cls.getName() + " not found. Starting one in new Thread...");
						connectionFuture = executor.submit(new Callable<Connection>() {
							public Connection call() {
								LOGGER.finer("Connection Thread started.");
								try {
									Connection connection = cls.getConstructor().newInstance();
									connection.establish();
									return connection;
								} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
										| InvocationTargetException | NoSuchMethodException | SecurityException e) {
									// will never happen, since Connection has a fitting Constructor and all classes used
									// here extend Connection.
									LOGGER.severe("Exception occurred during establishment of Connection to " + cls.getName());
									e.printStackTrace();
									return null;
								}
							}
						});
						connections.put(cls, connectionFuture);
					}
				}
				// get the Futures' responses in a second loop, so all of them are started before
				// the first blocking execution.
				for(Class<?extends Connection> cls: connectionRequest.getRequestedConnections()) {
					Future<Connection> connectionFuture = connections.get(cls);
					connectionResponse.addConnection(connectionFuture.get());
				}
				// Call endpoint with Response object.
				LOGGER.info("Response built. Returning...");
				return connectionResponse;
			}
		});
	}
}
