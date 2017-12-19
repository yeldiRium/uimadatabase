package dbtest.connection;

import java.util.HashSet;
import java.util.Set;

/**
 * Used to encapsulate Connection objects returned from the ConnectionManager.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ConnectionResponse
{
	protected HashSet<Connection> connections;

	public ConnectionResponse()
	{
		this.connections = new HashSet<>();
	}

	/**
	 * Add a connection object. If it is already there, nothing happens.
	 *
	 * @param connection
	 */
	public void addConnection(Connection connection)
	{
		if (!this.connections.contains(connection))
		{
			this.connections.add(connection);
		}
	}

	/**
	 * @return the stored connections.
	 */
	@SuppressWarnings("unchecked")
	public Set<Connection> getConnections()
	{
		return (Set<Connection>) this.connections.clone();
	}
}
