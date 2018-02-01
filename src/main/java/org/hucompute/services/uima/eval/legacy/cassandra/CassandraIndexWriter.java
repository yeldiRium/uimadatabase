package org.hucompute.services.uima.eval.legacy.cassandra;

import com.datastax.driver.core.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * here we write Cassandra indexes to our database. Since Cassandra is rather
 * slow with on the fly calculations and does not offer higher level
 * manipulations like views in relational databases we need to prepare super
 * columns which contains count informations. Calculating on query time would be
 * costly and would not perform well. Entry point for finishing the whole
 * keyspace is {@link #writeAllIndices()}.
 *
 * @author Luis Glaser
 */
public class CassandraIndexWriter
{

	/**
	 * this can be run before querying to ensure the database is ready for querying.
	 * If new tables are added on which queries are dependent, add them to
	 * requirements, otherwise they may not be found.
	 *
	 * @return true if database is queryable.
	 */
	static public boolean checkTableRequirements()
	{
		boolean check = true;
		String[] requirements = {"tokens", "ttr", "pos", "morph", "sentence", "tagsetdescription",
				"distinctlemmacounter", "tfidf", "wikidatahyponym", "dependency", "wikify", "xmi", "idf", "paragraph",
				"html", "normfreqlength", "lemma", "lemmacounter", "distinctlemmacounter"};
		try
		{
			Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			KeyspaceMetadata ks = cluster.getMetadata().getKeyspace("textimager");
			System.out.print("Searching for missing tables:\n[Missing] ");
			for (String table : requirements)
			{
				if (ks.getTable(table) == null)
				{
					System.out.print(table + " ");
					check = false;
				}
			}
			cluster.close();
		} catch (Exception e)
		{

		}
		System.out.println("\nDONE checking requirements.");
		return check;
	}

	/**
	 * Here we prepare out database for querying. Constructs persistent indices from
	 * existing data. Should be done before database is opened for the user, since
	 * it might be time consuming.
	 */
	static public void writeAllIndices()
	{
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect("textimager");
		session.execute("use textimager;");

		long start;
		long end;
		start = System.currentTimeMillis();
		System.out.println("Beginning to write all indices.. THIS MAY TAKE A WHILE.");
		writeLemmaCounter(session);
		writeDistinctLemmaCounter(session);
		writeLemmaOccurrences(session);
		writeTTR(session);
		writeFrequencyNormalizedByLength(session);
		writeIDF(session);
		writeTfIdf(session);
		end = System.currentTimeMillis();
		session.close();
		System.out.println("DONE writing Indices. It took " + (end - start) + " ms. You may now start querying.");
	}

	// ============================BUILDING COUNTERS REQUIRED FOR OTHER
	// FUNCTIONS============================

	/**
	 * This creates a table which counts each lemma in all documents which have been
	 * put into database Columns: text_xmi | lemma | No. of lemma in text_xmi
	 *
	 * @param session connection session to CassandraDB
	 */
	static public void writeLemmaCounter(Session session)
	{
		// first we create a table
		String createCounterQuery = "CREATE TABLE IF NOT EXISTS lemmaCounter (counter_value counter, xmi varchar, lemma varchar, PRIMARY KEY (xmi, lemma));";
		session.execute(createCounterQuery);
		String getLemmaQuery = "SELECT xmi, value from lemma;";
		ResultSet rs = session.execute(getLemmaQuery); // we extract all lemmata from their texts, NOT unique
		HashSet<SimpleStatement> batchSet = new HashSet<>();
		int counter = 0;
		System.out.println("Writing lemmaCounter.");
		for (Row row : rs)
		{
			String xmi = row.getString("xmi");
			String lemma = row.getString("value");
			batchSet.add(new SimpleStatement(
					"UPDATE lemmaCounter SET counter_value = counter_value + 1 WHERE xmi=? AND lemma=?;", xmi, lemma));
			counter++;
			if (counter >= 10000)
			{
				System.out.print("\nStarting queries");
				CassandraQueryUtil.batchQueries(session, batchSet, "c"); // "c" because we are producing a counter table
				// which needs different prepared Statements
				System.out.print("finished");
				counter = 0;
				batchSet.clear();
			}
		}
		CassandraQueryUtil.batchQueries(session, batchSet, "c");
		batchSet.clear();
		System.out.println("DONE writing lemmaCounter.");
	}

	/**
	 * This creates a table that holds the number of distinct lemmata in each
	 * document. Columns: text_xmi | No. of distinct lemmata in text_xmi
	 *
	 * @param session connection session to CassandraDB
	 */
	static public void writeDistinctLemmaCounter(Session session)
	{
		// first we create a table
		String createCounterQuery = "CREATE TABLE IF NOT EXISTS distinctLemmaCounter (xmi varchar, count_value int, PRIMARY KEY (xmi));";
		session.execute(createCounterQuery);
		HashMap<String, HashSet<String>> lemmaCount = new HashMap<>();
		String getLemmaQuery = "SELECT xmi, value FROM lemma;";
		// we extract all lemmata from their texts, NOT unique since not possible in
		// cassandra,
		// we will need to discard doubles ourselves internally.
		ResultSet rs = session.execute(getLemmaQuery);
		for (Row result : rs)
		{
			// for each document we collect lemmata in a set (eliminating doubles)
			String tempKey = result.getString("xmi");
			HashSet<String> temp = lemmaCount.getOrDefault(tempKey, new HashSet<>());
			temp.add(result.getString("value"));
			lemmaCount.put(tempKey, temp);
		}
		HashSet<SimpleStatement> batchSet = new HashSet<>();
		int counter = 0;
		for (Map.Entry<String, HashSet<String>> entry : lemmaCount.entrySet())
		{
			// writing the number of distinct lemmata for each document.
			batchSet.add(new SimpleStatement("INSERT INTO distinctLemmaCounter (xmi, count_value) VALUES (?, ?);",
					entry.getKey(), entry.getValue().size()));
			counter++;
			if (counter >= 10000)
			{
				System.out.print("\nStarting queries");
				CassandraQueryUtil.batchQueries(session, batchSet, "s");
				counter = 0;
				batchSet.clear();
			}
		}
		// we write leftover Statements to cassandra
		CassandraQueryUtil.batchQueries(session, batchSet, "s");
		batchSet.clear();
		System.out.print("DONE writing distinctLemmaCounter.");

	}
	// ============================FUNCTIONS FOR BUILDING INDICES FOR
	// TTR============================

	/**
	 * this can be used to write TTR indices into a cassandra base. Requires
	 * documents to have already been written into the database.
	 *
	 * @param session CassandraDB session
	 */
	public static void writeTTR(Session session)
	{
		String createTableTTR = "CREATE TABLE IF NOT EXISTS ttr (xmi varchar, value float, PRIMARY KEY (xmi));";
		session.execute(createTableTTR); // we first create the table
		HashMap<String, Integer> t = CassandraQueryUtil.getTokenCount(session);

		HashMap<String, Integer> l = CassandraQueryUtil.getLemmaCount(session);

		HashSet<SimpleStatement> batchSet = new HashSet<>();
		int counter = 0;
		System.out.println("Writing " + l.size() + " TTR indices to database..");
		for (Map.Entry<String, Integer> entry : l.entrySet())
		{
			String xmi = entry.getKey();
			// we're batching queries to improve write times.
			batchSet.add(new SimpleStatement("INSERT INTO ttr (xmi, value) VALUES (?, ?);", xmi,
					(float) entry.getValue() / (float) t.get(xmi)));
			counter++;
			if (counter >= 10000)
			{
				System.out.print("\nStarting queries");
				CassandraQueryUtil.batchQueries(session, batchSet, "s");
				counter = 0;
				batchSet.clear();
			}
		}
		// executing batchQueries with left over statements.
		CassandraQueryUtil.batchQueries(session, batchSet, "s");
		batchSet.clear();
		System.out.print("Finished writing TTR.");
	}

	// ============================WRITE VALUES REQUIRED FOR
	// TFIDF============================

	/**
	 * creates a table which contains each text with each lemma normalized by its
	 * text length. this is required by tf-idf. Requires
	 * {@link #writeLemmaCounter(Session)}
	 *
	 * @param session CassandraDB session
	 */
	static public void writeFrequencyNormalizedByLength(Session session)
	{
		SimpleStatement createTable = new SimpleStatement("CREATE TABLE IF NOT EXISTS normFreqLength"
				+ " (xmi varchar, lemma varchar, value float, PRIMARY KEY (xmi, lemma));");
		session.execute(createTable);
		HashMap<String, Double> textLenghts = CassandraQueryUtil.getTextLengths(session);
		SimpleStatement getLemma = new SimpleStatement("SELECT * FROM lemmacounter;");
		ResultSet rs = session.execute(getLemma);
		HashSet<SimpleStatement> batchSet = new HashSet<>();
		int counter = 0;
		for (Row row : rs)
		{
			String xmi = row.getString("xmi");
			String lemma = row.getString("lemma");
			Integer lemmaCount = (int) row.getLong("counter_value");
			batchSet.add(new SimpleStatement("INSERT INTO normFreqLength" + " (xmi, lemma, value) VALUES (?,?,?);", xmi,
					lemma, (float) (lemmaCount / textLenghts.get(xmi))));
			counter++;
			if (counter >= 10000)
			{
				System.out.print("\nStarting queries");
				CassandraQueryUtil.batchQueries(session, batchSet, "s");
				counter = 0;
				batchSet.clear();
			}
		}
		// using up left over batch queries
		CassandraQueryUtil.batchQueries(session, batchSet, "s");
		batchSet.clear();
		System.out.println("DONE writing frequencies normalized by length");
	}

	/**
	 * creates occurrences counts of lemmata in document database. needs to be
	 * rerun, if documents have been added also requires lemmaCounter table, written
	 * by {@link #writeLemmaCounter(Session)}
	 *
	 * @param session CassandraDB session
	 */
	static public void writeLemmaOccurrences(Session session)
	{
		SimpleStatement createTable = new SimpleStatement(
				"CREATE TABLE IF NOT EXISTS lemmaOccurrences" + "(lemma varchar, value int, PRIMARY KEY (lemma));");
		session.execute(createTable);
		HashMap<String, HashSet<String>> occurrences = new HashMap<>();
		SimpleStatement getOccurrencesBase = new SimpleStatement("SELECT xmi, lemma from lemmacounter;");
		ResultSet rs = session.execute(getOccurrencesBase);
		// we first extract all lemmata from database and count them within driver
		int counter = 0;
		System.out.println("Counting lemma occurences..");
		for (Row row : rs)
		{
			String lemma = row.getString("lemma");
			HashSet<String> seenXmi = occurrences.getOrDefault(lemma, new HashSet<>());
			seenXmi.add(row.getString("xmi"));
			occurrences.put(lemma, seenXmi);
			counter++;
			if (counter % 10000 == 0)
			{
				System.out.print(".");
			}
		}
		counter = 0;
		HashMap<String, Integer> counts = new HashMap<>();
		// collapsing HashMap
		for (String lemma : occurrences.keySet())
		{
			counts.put(lemma, occurrences.get(lemma).size());
		}

		// Writing to database
		String insert = "INSERT INTO lemmaOccurrences (lemma, value) VALUES (?,?);";
		HashSet<SimpleStatement> batchSet = new HashSet<>();
		for (Map.Entry<String, Integer> entry : counts.entrySet())
		{
			batchSet.add(new SimpleStatement(insert, entry.getKey(), entry.getValue()));
			counter++;
			if (counter >= 10000)
			{
				System.out.print("\nStarting queries");
				CassandraQueryUtil.batchQueries(session, batchSet, "s");
				counter = 0;
				batchSet.clear();
			}
		}
		// Using up left over batched insert queries.
		CassandraQueryUtil.batchQueries(session, batchSet, "s");
		System.out.print("DONE writing lemmaOccurrences.");

	}

	// ============================FUNCTIONS FOR BUILDING INDICES FOR
	// TF-IDF============================

	/**
	 * calculates and writes values for tf-idfs to database. REQUIRES normalized
	 * frequencies and idf tables written by
	 * {@link #writeFrequencyNormalizedByLength(Session)} and
	 * {@link #writeIDF(Session)} Columns: xmi | lemma | tfidf
	 *
	 * @param session CassandraDB session
	 */
	static public void writeTfIdf(Session session)
	{
		SimpleStatement createTfIdfQuery = new SimpleStatement(
				"CREATE TABLE IF NOT EXISTS tfidf (xmi varchar, lemma varchar, value float, PRIMARY KEY (xmi, lemma));");
		session.execute(createTfIdfQuery);
		HashMap<String, Float> idf = CassandraQueryUtil.getIDF(session);

		SimpleStatement getTF = new SimpleStatement("SELECT * FROM normFreqLength;");
		ResultSet rs = session.execute(getTF);
		HashSet<SimpleStatement> batchSet = new HashSet<>();
		int counter = 0;
		for (Row row : rs)
		{
			String xmi = row.getString("xmi");
			String lemma = row.getString("lemma");
			Float freq = (float) row.getFloat("value");
			batchSet.add(new SimpleStatement("INSERT INTO tfidf (xmi, lemma, value) VALUES (?,?,?);", xmi, lemma,
					(freq * (idf.get(lemma)))));
			counter++;
			if (counter >= 10000)
			{
				System.out.print("\nStarting queries");
				CassandraQueryUtil.batchQueries(session, batchSet, "s");
				counter = 0;
				batchSet.clear();
			}
		}
		CassandraQueryUtil.batchQueries(session, batchSet, "s");
		batchSet.clear();
		System.out.println("DONE writing tfidf");
	}

	/**
	 * writes IDF values to database. Requires
	 * {@link #writeLemmaOccurrences(Session)} to have been executed once.
	 *
	 * @param session CassandraDB session
	 */
	static public void writeIDF(Session session)
	{
		SimpleStatement createTable = new SimpleStatement(
				"CREATE TABLE IF NOT EXISTS idf" + "(lemma varchar, value float, PRIMARY KEY (lemma));");
		session.execute(createTable);

		SimpleStatement documentCount = new SimpleStatement("SELECT count(xmi) as c FROM xmi;");
		Integer docCount = 0;
		ResultSet dc = session.execute(documentCount);
		for (Row docRow : dc)
		{
			docCount = (int) docRow.getLong("c");

			SimpleStatement getLemmaOccurences = new SimpleStatement("SELECT * FROM lemmaOccurrences;");
			ResultSet lo = session.execute(getLemmaOccurences);

			// Creating HashMap for Querying
			HashSet<SimpleStatement> batchSet = new HashSet<>();
			int counter = 0;

			for (Row row : lo)
			{
				String lemma = row.getString("lemma");
				batchSet.add(new SimpleStatement("INSERT INTO idf (lemma, value) VALUES (?,?);", lemma,
						(float) Math.log(docCount / (row.getInt("value")))));
				counter++;
				if (counter >= 10000)
				{
					System.out.print("\nStarting queries");
					CassandraQueryUtil.batchQueries(session, batchSet, "s");
					counter = 0;
					batchSet.clear();
				}
			}
			CassandraQueryUtil.batchQueries(session, batchSet, "s");
			System.out.print("finished writing IDF.");

		}

	}

}
