package org.hucompute.services.uima.database;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hucompute.services.uima.database.cassandra.CassandraIndexWriter;
import org.hucompute.services.uima.database.cassandra.CassandraQueryHandler;
import org.hucompute.services.uima.database.neo4j.Neo4jQueryHandler;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * The entry point to query for data in all running databases. Currently we can
 * connect to a Neo4J and a CassandraDB instance. Extend
 * this and implement Callable to plug in another
 * database. Callable in {@link #stringRequestHandler(String)} returns first
 * valid result and ignores slow answers.
 * 
 * @author Manuel Stoeckel,Luis Glaser 
 * Created on 26 Sep 2017
 */
public abstract class RequestHandler {

	static public void main(String[] args) {
//		stringRequestHandler("TTR_all|TTR_one 1063|TTR_collection 1107 1108|count_type lEmma|count_type_in_document 1063 ToKen");
//		stringRequestHandler("count_type_with_value LemmA der|count_type_with_value_in_document 1108 LemmA der");
//		stringRequestHandler("get_termFrequency_logNorm 1108 der|get_termFrequency_doubleNorm 1108 der");
//		stringRequestHandler("get_termFrequencies 1108|get_documentsContaining Mensch");
//		stringRequestHandler("get_inverseDocumentFrequency Mensch|get_inverseDocumentFrequencies 1108");
//		stringRequestHandler("get_tfidf 1108 Mensch");
//		stringRequestHandler("get_tfidf_all 1190");
		stringRequestHandler("get_tfidf_all_all");
	}

	/**
	 * Expects input [FUNCTION_NAME(ARGS?)[|FUNCTION_NAME(ARGS?)..]].
	 * 
	 * @param query the full query in a single string, multiples separated by "|"
	 * @return ArrayList of Arrays of Strings with commands
	 */
	static public ArrayList<String[]> parse(String query) {
		ArrayList<String[]> queries = new ArrayList<>();
		for (String line : query.split("\\|")) {
			queries.add(line.split("\\s"));
		}
		return queries;
	}

	// ENTRYPOINT
	/**
	 * Parses String, runs commands.
	 * <p>Multiple Commands have to be split by "|"</p>
	 * @param arg String containing all commands and arguments.
	 * @return a {@link JSONArray} containing all responses as {@link JSONObject}.
	 */
	static public JSONArray stringRequestHandler(String arg) {
		// Checks if Cassandra has required tables for querying. If not, creates them
		try {
			if (!CassandraIndexWriter.checkTableRequirements()) {
				CassandraIndexWriter.writeAllIndices();
			}
		} catch (NoHostAvailableException e) {
			System.out.println(e.toString());
		}
		JSONArray ret = new JSONArray();
		for (String[] query : parse(arg)) {
			ExecutorService executor = Executors.newWorkStealingPool();
			List<Callable<JSONObject>> callables = new ArrayList<>();

			/* Cassandra callable */
			// TODO:check if online
			try {
				callables.add(new CassandraQueryHandler(query));
			} catch (NoHostAvailableException e) {
				System.out.println(e.toString());
			}

			/* Neo4j callable */
			// TODO:check if online
			try {
				callables.add(new Neo4jQueryHandler(query));
			} catch (ServiceUnavailableException e) {
				System.out.println(e.toString());
			}

			/* The actual magic */
			try {
				JSONObject result = executor.invokeAny(callables);

				ret.put(result);
			} catch (IllegalArgumentException | InterruptedException | ExecutionException e) {
				if (e.getClass() == IllegalArgumentException.class) {
					System.out.println("All databases down!");
					break;
				} else {
					e.printStackTrace();
				}
			}
		}
		// Debug
		System.out.println(ret);
		return ret;
	}
}
