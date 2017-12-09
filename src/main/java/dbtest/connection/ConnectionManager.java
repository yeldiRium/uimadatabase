package dbtest.connection;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class ConnectionManager {
	/**
	 * How long the Thread should wait between checking the connections.
	 */
	protected final int sleepTime = 200;
	protected Map<ConnectionRequest, ConnectionResponse> connectionRequests;
	protected Map<Class<?extends Connection>, Connection> connections;
	protected Thread checkThread;
	
	protected class ConnectionChecker implements Runnable {
		protected boolean interrupted = false;
		
		/**
		 * Regularly processes all current requests.
		 * If there are no more requests afterwards, the Thread is stopped.
		 */
		@Override
		public void run() {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					for(ConnectionRequest connectionRequest: connectionRequests.keySet()) {
						processRequest(connectionRequest);
					}
					// If there are no requests pending, stop the watching thread.
					// It will be restarted once new requests come in.
					if(connectionRequests.size() == 0) {
						throw new InterruptedException();
					}
					Thread.sleep(sleepTime);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					this.interrupted = true;
				}
			}
		}
	}
	
	public ConnectionManager() {
		this.connectionRequests = new HashMap<>();
		this.connections = new HashMap<>();
	}
	
	/**
	 * Starts a watching thread that checks connections for their status.
	 * Will be stopped as soon as there are no more requests to process.
	 */
	public void watchConnections() {
		if((this.checkThread == null) || (!this.checkThread.isAlive())) {
			ConnectionChecker checker = new ConnectionChecker();
			this.checkThread = new Thread(checker);
			this.checkThread.start();
		}
	}

	/**
	 * Adds a request for processing.
	 * Creates an empty Response object.
	 * 
	 * Starts the watching thread, if there is none running currently.
	 * @param connectionRequest
	 */
	public void submitRequest(ConnectionRequest connectionRequest) {
		if(this.connectionRequests.get(connectionRequest) != null) {
			throw new ConnectionRequestAlreadySubmittedException();
		}
		ConnectionResponse connectionResponse = new ConnectionResponse();
		
		this.connectionRequests.put(connectionRequest, connectionResponse);
		this.processRequest(connectionRequest);
		
		// Watch connections after first processing.
		this.watchConnections();
	}
	
	/**
	 * Checks all connections the given Request wants.
	 * If a connection is not registered yet, it will be created and started to establish.
	 * If a connection is already established, it will be added to the according response object.
	 * If all connections are established, the Response is finished and the responseEndpoint called.
	 * @param connectionRequest
	 */
	protected void processRequest(ConnectionRequest connectionRequest) {
		ConnectionResponse connectionResponse = this.connectionRequests.get(connectionRequest);
		// Will be set to false by any not yet established connection.
		boolean isRequestComplete = true;
		for(Class<?extends Connection> cls: connectionRequest.getRequestedConnections()) {
			Connection connection = this.connections.get(cls);
			if(connection == null) {
				try {
					connection = cls.getConstructor().newInstance();
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
						| InvocationTargetException | NoSuchMethodException | SecurityException e) {
					// will never happen, since Connection has a fitting Constructor and all classes used
					// here extend Connection.
					e.printStackTrace();
				}
				this.connections.put(cls, connection);
				
				// This starts a thread that tries to connect several times.
				// There will be no immediate effect.
				connection.establish();
			}
			// Will usually not be true, if the above block was executed.
			// There is no harm in testing it immediately, though.
			if(connection.isEstablished()) {
				connectionResponse.addConnection(connection);
				isRequestComplete &= true;
				continue;
			}
			isRequestComplete &= false;
		}
		if(isRequestComplete) {
			// Call endpoint with Response object.
			connectionResponse.finish();
			connectionRequest.getResponseEndpoint().acceptResponse(connectionResponse);
			this.connectionRequests.remove(connectionRequest);
		}
	}
}
