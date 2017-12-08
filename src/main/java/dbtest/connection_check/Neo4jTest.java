package dbtest.connection_check;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

/**
 * Tests the connection to the Neo4J container.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class Neo4jTest extends DBConnectionTest {
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
