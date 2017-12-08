package dbtest.connection_check;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Tests the connection to the MySQL container.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class MySQLTest extends DBConnectionTest {
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
		} catch (SQLException e) {
			System.out.println("MySQL: Connection failed. Retrying...");
			System.out.println("MySQL: Error was \"" + e.getMessage() + "\"");
		}
	}
}
