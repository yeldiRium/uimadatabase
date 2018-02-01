package org.hucompute.services.uima.eval.database.connection;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The ConnectionRequest is used to transfer a number of Connection classes to the ConnectionManager.
 * It tells the Manager, which Connections should be prepared and who should be called with them.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ConnectionRequest
{
	private HashSet<Class<? extends Connection>> connections;

	/**
	 * Constructs the ConnectionRequest from an endpoint and creates an empty list for the connection classes.
	 */
	public ConnectionRequest()
	{
		this.connections = new HashSet<>();
	}

	/**
	 * Constructs the ConnectionRequest from an endpoint and a number of connection classes.
	 *
	 * @param connectionClasses A number of classes for which a connection should be established.
	 */
	@SafeVarargs
	public ConnectionRequest(Class<? extends Connection>... connectionClasses)
	{
		this.connections = new HashSet<>();
		this.connections.addAll(Arrays.asList(connectionClasses));
	}

	/**
	 * Adds a connectionClass to the set.
	 *
	 * @param connectionClass A class for which a connection should be established.
	 */
	public void addRequestedConnection(Class<? extends Connection> connectionClass)
	{
		this.connections.add(connectionClass);
	}

	/**
	 * @return the set of connection classes.
	 */
	@SuppressWarnings("unchecked")
	public Set<Class<? extends Connection>> getRequestedConnections()
	{
		return (Set<Class<? extends Connection>>) this.connections.clone();
	}
}
