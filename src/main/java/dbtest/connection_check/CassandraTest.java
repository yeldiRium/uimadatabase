package dbtest.connection_check;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Tests the connection to the Cassandra container.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class CassandraTest extends DBConnectionTest {
	protected Cluster cluster;
	protected Session session;
	@Override
	protected void tryToConnect() {
		// If the connection is already established, nothing further has to be done
		if(this.session != null) {
			System.out.println("Cassandra: connected.");
			return;
		}
		
		try {
			// Create cluster with credentials from environment variables
			this.cluster = Cluster.builder()
				.addContactPoint(System.getenv("CASSANDRA_HOST"))
				.withPort(Integer.parseInt(System.getenv("CASSANDRA_PORT")))
				.withCredentials(System.getenv("CASSANDRA_USER"), System.getenv("CASSANDRA_PASS"))
				.build();
			// Connect to server
			this.session = cluster.connect();
			System.out.println("Cassandra: Connection successful!");
		} catch(NoHostAvailableException|AuthenticationException|IllegalStateException e) {
			System.out.println("Cassandra: Connection failed. Retrying...");
		}
	}
}
