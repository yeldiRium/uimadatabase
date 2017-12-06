package dbtest;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.basex.api.client.ClientSession;

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
		@Override
		protected void tryToConnect() {
			System.out.println("ArangoDBTest running...");
		}
	}
	
	/**
	 * Tests the connection to the BaseX container.
	 */
	private class BaseXTest extends DBConnectionTest {
		@Override
		protected void tryToConnect() {
			try {
				final String host = System.getenv("BASEX_HOST");
				final int port = Integer.parseInt(System.getenv("BASEX_PORT"));
				final String username = System.getenv("BASEX_USER");
				final String password = System.getenv("BASEX_PASS");
				ClientSession session = new ClientSession(host, port, username, password);
				System.out.println("Connection to BaseXHTTP Server successful!");
				Thread.currentThread().interrupt();
			} catch (IOException e) {
				System.out.println("Connection to BaseXHTTP Server could not be established. Retrying...");
			}
		}
	}
	
	/**
	 * Tests the connection to the Cassandra container.
	 */
	private class CassandraTest extends DBConnectionTest {
		@Override
		protected void tryToConnect() {
			System.out.println("CassandraTest running...");
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
