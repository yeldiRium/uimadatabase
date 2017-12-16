package dbtest.connection.implementation;

import java.io.IOException;

import org.basex.api.client.ClientSession;

import dbtest.connection.Connection;

/**
 * Establishes and exposes a connection to the BaseX Server.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class BaseXConnection extends Connection {
	protected ClientSession session;

	@Override
	protected boolean tryToConnect() {
		// If the connection is already established, nothing further has to be done
		if(this.session != null) {
			return true;
		}
		
		try {
			final String host = System.getenv("BASEX_HOST");
			final int port = Integer.parseInt(System.getenv("BASEX_PORT"));
			final String username = System.getenv("BASEX_USER");
			final String password = System.getenv("BASEX_PASS");
			// Create client with credentials from environment variable
			this.session = new ClientSession(host, port, username, password);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public void close()
	{
		// TODO: implement
	}

}
