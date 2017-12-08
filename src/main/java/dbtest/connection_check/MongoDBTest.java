package dbtest.connection_check;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * Tests the connection to the Mongo DB container.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class MongoDBTest extends DBConnectionTest {
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
