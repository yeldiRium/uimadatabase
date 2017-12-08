package dbtest.connection_check;

import java.util.HashSet;
import java.util.Set;

/**
 * This tests the connection to all configured databases.
 * It will retry an infinite amount of times and report each time if it was successful or not.
 * 
 * This is meant to test if the docker setup works correctly.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class Main {
	public static void main(String[] args) {
		// Create a set of all test Runnables so that they can be started in a simple loop.
		Set<Runnable> list = new HashSet<>();
		list.add(new ArangoDBTest());
		list.add(new BaseXTest());
		list.add(new CassandraTest());
		list.add(new MongoDBTest());
		list.add(new MySQLTest());
		list.add(new Neo4jTest());
		
		// Iterate over the Runnables
		for (Runnable rnbl: list) {
			// create Thread and start
			(new Thread(rnbl)).start();
		}
	}
}
