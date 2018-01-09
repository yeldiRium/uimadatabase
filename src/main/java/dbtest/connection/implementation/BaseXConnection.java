package dbtest.connection.implementation;

import dbtest.connection.Connection;
import org.basex.api.client.ClientSession;

import java.io.IOException;

/**
 * Establishes and exposes a connection to the BaseX Server.
 *
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class BaseXConnection extends Connection
{
	protected ClientSession session;

	public ClientSession getSession()
	{
		return this.session;
	}

	@Override
	protected boolean tryToConnect()
	{
		// If the connection is already established, nothing further has to be
		// done
		if (this.session != null)
		{
			return true;
		}

		try
		{
			final String host = System.getenv("BASEX_HOST");
			final int port = Integer.parseInt(System.getenv("BASEX_PORT"));
			final String username = System.getenv("BASEX_USER");
			final String password = System.getenv("BASEX_PASS");
			// Create client with credentials from environment variable
			this.session = new ClientSession(host, port, username, password);
			return true;
		} catch (IOException e)
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
		try
		{
			this.session.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

}
