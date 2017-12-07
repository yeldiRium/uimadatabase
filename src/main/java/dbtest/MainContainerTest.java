package dbtest;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.basex.api.client.ClientSession;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.ArangoDBVersion;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * This tests the connection to all configured databases.
 * It will retry an infinite amount of times and report each time if it was successful or not.
 * 
 * This is a test used with docker to check if everything is working.
 * 
 * It will be the main class of the compiled jar for the time being and be replaced, once everything works
 * and more functionality exists.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class MainContainerTest {
	public static void main(String[] args) {
		MainContainerTest outer = new MainContainerTest();
		// Create a set of all test Runnables so that they can be started in a simple loop.
		Set<Runnable> list = new HashSet<>();
		list.add(outer.new ArangoDBTest());
		list.add(outer.new BaseXTest());
		list.add(outer.new CassandraTest());
		list.add(outer.new MongoDBTest());
		list.add(outer.new MySQLTest());
		list.add(outer.new Neo4JTest());
		
		// Iterate over the Runnables
		for (Runnable rnbl: list) {
			// create Thread and start
			(new Thread(rnbl)).start();
		}
	}
	
	/**
	 * Start a loop which calls a protected method every second, which will be
	 * used to try and connect to the implementation specific database container.
	 */
	private abstract class DBConnectionTest implements Runnable {
		@Override
		public void run() {	
			// Allow interruption of thread from outside.
			while (!Thread.currentThread().isInterrupted()) {
				try {
					this.tryToConnect();
					Thread.sleep(1000);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		}
		
		protected abstract void tryToConnect();
	}
	
	/**
	 * Tests the connection to the Arango DB container.
	 */
	private class ArangoDBTest extends DBConnectionTest {
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
				this.version = arangoDB.getVersion();
				System.out.println("ArangoDB: Connection successful!");
				System.out.println("ArangoDB: Server is called \"" + version.getServer() + "\" with version " + version.getVersion());
			} catch (ArangoDBException e) {
				System.out.println("ArangoDB: Connection failed. Retrying...");
			}
		}
	}
	
	/**
	 * Tests the connection to the BaseX container.
	 */
	private class BaseXTest extends DBConnectionTest {
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
	
	/**
	 * Tests the connection to the Cassandra container.
	 */
	private class CassandraTest extends DBConnectionTest {
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
	
	/**
	 * Tests the connection to the Mongo DB container.
	 */
	private class MongoDBTest extends DBConnectionTest {
		@Override
		protected void tryToConnect() {
			System.out.println("MongoDBTest running...");
		}
	}
	
	/**
	 * Tests the connection to the MySQL container.
	 */
	private class MySQLTest extends DBConnectionTest {
		@Override
		protected void tryToConnect() {
			System.out.println("MySQLTest running...");
		}
	}
	
	/**
	 * Tests the connection to the Neo4J container.
	 */
	private class Neo4JTest extends DBConnectionTest {
		@Override
		protected void tryToConnect() {
			System.out.println("Neo4JTest running...");
		}
	}
}
