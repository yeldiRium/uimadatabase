package dbtest.connection;

/**
 * A class that `AcceptsConnectionResponse`s can receive a `ConnectionResponse`
 * from the `ConnectionManager`, after a `ConnectionRequest` was processed and
 * completed.
 * 
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 */
public interface AcceptsConnectionResponse {
	/**
	 * Is called, after a Previous Request was finished.
	 * @param response
	 */
	void acceptResponse(ConnectionResponse response);
}
