package dbtest.connection_check;

import java.io.IOException;

import org.basex.api.client.ClientSession;

/**
 * Tests the connection to the BaseX container.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class BaseXTest extends DBConnectionTest {
	protected ClientSession session;
	@Override
	protected void tryToConnect() {
		// If the connection is already established, nothing further has to be done
		if(this.session != null) {
			System.out.println("BaseX: Connected.");
			return;
		}
		
		try {
			final String host = System.getenv("BASEX_HOST");
			final int port = Integer.parseInt(System.getenv("BASEX_PORT"));
			final String username = System.getenv("BASEX_USER");
			final String password = System.getenv("BASEX_PASS");
			// Create client with credentials from environment variable
			this.session = new ClientSession(host, port, username, password);
			System.out.println("BaseX: Connection successful!");
		} catch (IOException e) {
			System.out.println("BaseX: Connection failed. Retrying...");
		}
	}
}