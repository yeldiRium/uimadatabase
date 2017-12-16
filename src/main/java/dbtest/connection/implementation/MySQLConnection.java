package dbtest.connection.implementation;

import java.sql.DriverManager;
import java.sql.SQLException;

import dbtest.connection.Connection;

/**
 * Establishes and exposes a connection to the MySQL Server.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public class MySQLConnection extends Connection {
	protected java.sql.Connection connection;

	@Override
	protected boolean tryToConnect() {
		// If the connection is already established, nothing further has to be done
		if(this.connection != null) {
			return true;
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
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	@Override
	public void close()
	{
		// TODO: implement
	}
}
