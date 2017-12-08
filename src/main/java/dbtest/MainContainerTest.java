package dbtest;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.basex.api.client.ClientSession;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.ArangoDBVersion;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

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
		list.add(outer.new Neo4jTest());
		
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
		protected MongoClient client;
		@Override
		protected void tryToConnect() {
			// If the connection is already established, nothing further has to be done
			if(this.client != null) {
				System.out.println("MongoDB: connected.");
				return;
			}
			
			try {
				this.client = new MongoClient(
					System.getenv("MONGODB_HOST"),
					Integer.parseInt(System.getenv("MONGODB_PORT"))
				);
				System.out.println("MongoDB: Connection successful!");
			} catch(MongoException e) {
				System.out.println("MongoDB: Connection failed. Retrying...");
			}
		}
	}
	
	/**
	 * Tests the connection to the MySQL container.
	 */
	private class MySQLTest extends DBConnectionTest {
		protected Connection connection;
		@Override
		protected void tryToConnect() {
			// If the connection is already established, nothing further has to be done
			if(this.connection != null) {
				System.out.println("MySQL: connected.");
				return;
			}
			
			try {
				String host = System.getenv("MYSQL_HOST");
				String port = System.getenv("MYSQL_PORT");
				String dbname = System.getenv("MYSQL_DBNAME");
				String username = System.getenv("MYSQL_USER");
				String password = System.getenv("MYSQL_PASS");
				
				// MySQL Driver needs some strange url for connection, so we build it:
				String url = "jdbc:mysql://" + host + ":" + port + "/" + dbname + "?useSSL=false";
				
				this.connection = DriverManager.getConnection(
					url,
					username,
					password
				);
				System.out.println("MySQL: Connection successful!");
			} catch (SQLException|ClassNotFoundException e) {
				System.out.println("MySQL: Connection failed. Retrying...");
				System.out.println("MySQL: Error was \"" + e.getMessage() + "\"");
			}
		}
	}
	
	/**
	 * Tests the connection to the Neo4J container.
	 */
	private class Neo4jTest extends DBConnectionTest {
		protected Driver driver;
		@Override
		protected void tryToConnect() {
			// If the connection is already established, nothing further has to be done
			if(this.driver!= null) {
				System.out.println("Neo4j: connected.");
				return;
			}
			
			try {
				String host = System.getenv("NEO4J_HOST");
				String port = System.getenv("NEO4J_PORT");
				String username = System.getenv("NEO4J_USER");
				String password = System.getenv("NEO4J_PASS");
				
				this.driver= GraphDatabase.driver(
					"bolt://" + host + ":" + port,
					AuthTokens.basic(username, password)
				);
				System.out.println("Neo4j: Connection successful!");
			} catch(ServiceUnavailableException e) {
				System.out.println("Neo4j: Connection failed. Retrying...");
			}
		}
	}
}
