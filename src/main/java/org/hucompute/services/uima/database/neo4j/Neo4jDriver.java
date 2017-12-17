package org.hucompute.services.uima.database.neo4j;

import org.hucompute.services.uima.database.neo4j.impl.MDB_Neo4J_Impl;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

/**
 * Driver class that handles the actual interaction with the Neo4j server.
 * Provides methods for offline and online querying.
 * @author Manuel Stoeckel
 * Created on 21.06.2017
 */
public class Neo4jDriver implements AutoCloseable {

	private final Driver driver;

	/**
	 * Constructor.
	 * @param uri the bolt URI of the running Neo4j server.
	 * @param user the user name.
	 * @param password the corresponding password
	 */
	public Neo4jDriver (String uri, String user, String password) {
		driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
	}
	
	/**
	 * If no password is provided, "abc" is used per default.
	 * @param uri the bolt URI of the running Neo4j server.
	 */
	public Neo4jDriver (String uri) {
		driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "abc"));
	}
	
	/**
	 * If neither URI, nor password are provided, localhost:defaultPort and "abc" are used per default.
	 */
	public Neo4jDriver () {
		driver = GraphDatabase.driver("bolt://127.0.0.1:7687", AuthTokens.basic("neo4j", "abc"));
	}
	
	/**
	 * This method is a stub. The Exception should be changed to a unique subclass.
	 */
	@Override
	public void close() throws Exception {
		driver.close();
	}
	
	/**
	 * Runs a simple query on the running server.
	 * <p>Uses a all-purpose write session. A read-only variant is not yet implemented.</p>
	 * <p>The run command does not get appropriate safety parameters yet.</p>
	 * @param query a cypher query string.
	 * @return the result of the query.
	 */
	public StatementResult runSimpleQuery (final String query) {
        try ( Session session = driver.session() )
        {
	    	//TODO:Add parameters to run
        	System.out.println(query);
	        return session.run(query);
        }
	}

	/**
	 * Execute a simple query on the offline server using the GraphDatabaseService.
	 * <p>The execute command does not get appropriate safety parameters yet.</p>
	 * @param query a cypher query string.
	 * @return the result of the query.
	 */
	public static Result executeSimpleQuery (final String query) {
        try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
        {
        	long startTime = System.currentTimeMillis();
	    	//TODO:Add parameters to execute
        	System.out.println(query);
	        Result result = MDB_Neo4J_Impl.gdbs.execute(query);
	        tx.success();
	        System.out.println("Query took: " + (System.currentTimeMillis()-startTime) + " ms.");
			return result;
        }
	}
}
