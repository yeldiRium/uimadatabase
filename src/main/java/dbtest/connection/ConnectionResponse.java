package dbtest.connection;

import java.util.Collection;
import java.util.HashMap;

/**
 * Used to encapsulate Connection objects returned from the ConnectionManager.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ConnectionResponse
{
	protected HashMap<Class<? extends Connection>, Connection> connections;

	public ConnectionResponse()
	{
		this.connections = new HashMap<>();
	}

	/**
	 * Add a connection object. If it is already there, nothing happens.
	 *
	 * @param connection Established connection object.
	 */
	public void addConnection(Connection connection)
	{
		this.connections.put(connection.getClass(), connection);
	}

	/**
	 * @return the stored connections.
	 */
	public Collection<Connection> getConnections()
	{
		return this.connections.values();
	}

	/**
	 * @param className The name of a connection class.
	 * @return the stored connection.
	 */
	public Connection getConnection(Class<? extends Connection> className)
	{
		return this.connections.get(className);
	}
}
