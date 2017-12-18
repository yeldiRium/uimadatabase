package dbtest.evaluations;

import com.datastax.driver.core.Session;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.CassandraConnection;
import dbtest.evaluationFramework.EvaluationCase;
import org.hucompute.services.uima.database.cassandra.CassandraIndexWriter;

import java.io.*;
import java.util.Set;

/**
 * Benchmarks operations on existing data.
 * Assumes that content in tables 'lemma' and 'text_xmi' exists.
 * Also several operations depend on other operation's results to be present in
 * the database.
 * Thus order should be preserved or dependencies analyzed when refactoring.
 */
public class CassandraCounterCreateEvaluationCase implements EvaluationCase
{
	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(CassandraConnection.class);
		return connectionRequest;
	}

	@Override
	public void run(ConnectionResponse connectionResponse)
	{
		Set<Connection> connections = connectionResponse.getConnections();
		CassandraConnection cassandraConnection = (CassandraConnection) connections.iterator().next();
		Writer output;
		try
		{
			output = new PrintWriter(new FileOutputStream(new File("output/CassandraCounterCreateEvaluationCase.txt")));

			Session session = cassandraConnection.getSession();

			long start;
			long end;

			session.execute("create keyspace if not exists textimager with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
			session.execute("use textimager;");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeLemmaCounter(session);
			end = System.currentTimeMillis();
			output.write("Creating lemmaCounter took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeDistinctLemmaCounter(session);
			end = System.currentTimeMillis();
			output.write("Creating distinctLemmaCounter took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeTTR(session);
			end = System.currentTimeMillis();
			output.write("Creating TTR took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
//			HashMap<String, Float> ttr = CassandraIndexWriter.getTTR(session);
//			output.write();(ttr);
			end = System.currentTimeMillis();
			output.write("Getting TTR took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeLemmaOccurrences(session);
			end = System.currentTimeMillis();
			output.write("Writing lemmaOccurences took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeIDF(session);
			end = System.currentTimeMillis();
			output.write("Writing idf took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeFrequencyNormalizedByLength(session);
			end = System.currentTimeMillis();
			output.write("Writing tf took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeTfIdf(session);
			end = System.currentTimeMillis();
			output.write("Writing tf-idf took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
//			CassandraIndexWriter.getTFIDF(session);
			end = System.currentTimeMillis();
			output.write("Reading tf-idf took: "+(end-start)+"ms.");

			session.execute("drop keyspace textimager;");
			output.write("!!!Dropped keyspace!!!");
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
