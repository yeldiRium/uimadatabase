package dbtest.connection_check;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.ArangoDBVersion;

/**
 * Tests the connection to the Arango DB container.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class ArangoDBTest extends DBConnectionTest {
	protected ArangoDB arangoDB;
	protected ArangoDBVersion version;
	@Override
	protected void tryToConnect() {
		// If the connection is already established, nothing further has to be done
		// has to check connection, since the ArangoDB object can be created while
		// the actual connection when calling getVersion fails
		if(this.version != null) {
			System.out.println("ArangoDB: Connected.");
			return;
		}
		
		try {
			// Connect to ArangoDB with credentials from environment variables
			this.arangoDB = new ArangoDB.Builder()
				.host(
					System.getenv("ARANGODB_HOST"),
					Integer.parseInt(System.getenv("ARANGODB_PORT"))
				)
				.user(System.getenv("ARANGODB_USER"))
				.password(System.getenv("ARANGODB_PASS"))
				.build();
			// Request version to check, if the connection was successful
			// Without executing anything, the connection is not actually established
			// If the server is not reachable, this will log a ConnectException, which is
			// handled by arango and can't be prevented
			// So that will always pollute the logs.
			this.version = arangoDB.getVersion();
			System.out.println("ArangoDB: Connection successful!");
			System.out.println("ArangoDB: Server is called \"" + version.getServer() + "\" with version " + version.getVersion());
		} catch (ArangoDBException e) {
			System.out.println("ArangoDB: Connection failed. Retrying...");
		}
	}
}
