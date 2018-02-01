package org.hucompute.services.uima.legacy.cassandra;

import com.datastax.driver.core.*;

import java.util.HashMap;
import java.util.HashSet;

/**
 * contains utility functions to aid interaction with cassandraDB
 *
 * @author Luis Glaser
 */
public class CassandraQueryUtil
{

	/**
	 * avoid instantiating by making constructor private
	 */
	private CassandraQueryUtil()
	{

	}

	/**
	 * returns the length of each document by counting tokens from each document. no
	 * prerequisites.
	 *
	 * @param session CassandraDB session
	 * @return HashMap(DocumentXMI - & gt ; length)
	 */
	static public HashMap<String, Double> getTextLengths(Session session)
	{
		String query = "SELECT xmi, COUNT(start) AS length FROM tokens GROUP BY xmi;";

		ResultSet resultSet = session.execute(query);
		HashMap<String, Double> lengths = new HashMap<>();
		for (Row row : resultSet)
		{
			lengths.put(row.getString("xmi"), (double) row.getLong("length"));
		}
		return lengths;
	}

	/**
	 * batchQueries executes a collection of CQL Statements in order to improve
	 * performance. We can provide a counter switch, since counter columns are
	 * faster but need a a seperated PreparedStatement class.
	 *
	 * @param session  CassandraDB session
	 * @param batchSet prepared Statements in a Set
	 * @param setting  either "c" for counterStatements or nothing.
	 */
	static void batchQueries(Session session, HashSet<SimpleStatement> batchSet, String setting)
	{
		BatchStatement stmt;
		if (setting.equals("c"))
		{
			stmt = new BatchStatement(BatchStatement.Type.COUNTER);
		} else
		{
			stmt = new BatchStatement();
		}

		int count = 0;
		System.out.print("Batch execute");
		for (SimpleStatement s : batchSet)
		{
			stmt.add(s);
			count++;
			if (count == 500)
			{
				session.execute(stmt);
				System.out.print(".");
				stmt.clear();
				count = 0;
			}
		}

		session.execute(stmt);
		System.out.println("finished.");
	}

	/**
	 * For each document in the database we count distinct lemmata. REQUIRES TABLE
	 * distinctLemmaCounter, written by
	 * {@link CassandraIndexWriter#writeDistinctLemmaCounter(Session)} e.g.
	 * getLemmaCount(A,B,B,C,C,C) = 3.
	 *
	 * @param session CassandraDB session
	 * @return Map(documentID - & gt ; count of distinct lemmata)
	 */
	public static HashMap<String, Integer> getLemmaCount(Session session)
	{
		HashMap<String, Integer> lemmaCount = new HashMap<>();
		String query = "SELECT xmi, count_value FROM distinctLemmaCounter;";
		ResultSet rs = session.execute(query);
		for (Row result : rs)
		{
			lemmaCount.put(result.getString("xmi"), result.getInt("count_value"));
		}
		return lemmaCount;
	}

	/**
	 * For each document in the database we count tokens (equals length of
	 * document). e.g. getTokenCount(A,B,B,C,C,C) = 6.
	 *
	 * @param session CassandraDB session
	 * @return HashMap(documentID - & gt ; count of tokens)
	 */
	static public HashMap<String, Integer> getTokenCount(Session session)
	{
		HashMap<String, Integer> tokenCount = new HashMap<>();
		String query = "SELECT xmi, count(xmi) as count from tokens group by xmi;";
		ResultSet rs = session.execute(query);
		for (Row result : rs)
		{
			tokenCount.put(result.getString("xmi"), Long.valueOf(result.getLong("count")).intValue()); // cassandra
			// returns Long,
			// thus we cast.
		}
		return tokenCount;
	}

	// ============================QUERIES FROM DATABASES============================
	// ONLY USE FOR DEVELOPMENT

	/**
	 * simple getter for all IDF in database
	 *
	 * @param session CassandraDB session
	 * @return HashMap(DocumentXMI - & gt ; idf)
	 */
	static HashMap<String, Float> getIDF(Session session)
	{
		HashMap<String, Float> idf = new HashMap<>();
		SimpleStatement getIDF = new SimpleStatement("SELECT * FROM idf;");
		ResultSet rs = session.execute(getIDF);
		for (Row row : rs)
		{
			idf.put(row.getString("lemma"), row.getFloat("value"));
		}
		return idf;
	}

}
