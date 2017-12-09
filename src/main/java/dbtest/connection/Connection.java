package dbtest.connection;

/**
 * Subclasses MUST have a constructor that takes no arguments.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public abstract class Connection {
	public void establish() {
		
	}
	
	public boolean isEstablished() {
		return false;
	}
}
