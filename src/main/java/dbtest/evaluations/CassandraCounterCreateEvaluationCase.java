package dbtest.evaluations;

import com.datastax.driver.core.Session;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.CassandraConnection;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import org.apache.commons.io.FileUtils;
import org.hucompute.services.uima.database.cassandra.CassandraIndexWriter;

import java.io.File;
import java.io.IOException;
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
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	)
	{
		Set<Connection> connections = connectionResponse.getConnections();
		CassandraConnection cassandraConnection = (CassandraConnection) connections.iterator().next();
		StringBuilder builder = new StringBuilder();

		Session session = cassandraConnection.getSession();

		long start;
		long end;

		session.execute("create keyspace if not exists textimager with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
		session.execute("use textimager;");

		start = System.currentTimeMillis();
		CassandraIndexWriter.writeLemmaCounter(session);
		end = System.currentTimeMillis();
		builder.append("Creating lemmaCounter took: ")
				.append(end - start)
				.append("ms.");

		start = System.currentTimeMillis();
		CassandraIndexWriter.writeDistinctLemmaCounter(session);
		end = System.currentTimeMillis();
		builder.append("Creating distinctLemmaCounter took: ")
				.append(end - start)
				.append("ms.");

		start = System.currentTimeMillis();
		CassandraIndexWriter.writeTTR(session);
		end = System.currentTimeMillis();
		builder.append("Creating TTR took: ")
				.append(end - start)
				.append("ms.");

		start = System.currentTimeMillis();
//			HashMap<String, Float> ttr = CassandraIndexWriter.getTTR(session);
//			output.append();(ttr);
		end = System.currentTimeMillis();
		builder.append("Getting TTR took: ")
				.append(end - start)
				.append("ms.");

		start = System.currentTimeMillis();
		CassandraIndexWriter.writeLemmaOccurrences(session);
		end = System.currentTimeMillis();
		builder.append("Writing lemmaOccurences took: ")
				.append(end - start)
				.append("ms.");

		start = System.currentTimeMillis();
		CassandraIndexWriter.writeIDF(session);
		end = System.currentTimeMillis();
		builder.append("Writing idf took: ")
				.append(end - start)
				.append("ms.");

		start = System.currentTimeMillis();
		CassandraIndexWriter.writeFrequencyNormalizedByLength(session);
		end = System.currentTimeMillis();
		builder.append("Writing tf took: ")
				.append(end - start)
				.append("ms.");

		start = System.currentTimeMillis();
		CassandraIndexWriter.writeTfIdf(session);
		end = System.currentTimeMillis();
		builder.append("Writing tf-idf took: ")
				.append(end - start)
				.append("ms.");

		start = System.currentTimeMillis();
//			CassandraIndexWriter.getTFIDF(session);
		end = System.currentTimeMillis();
		builder.append("Reading tf-idf took: ")
				.append(end - start)
				.append("ms.");

		session.execute("drop keyspace textimager;");
		builder.append("!!!Dropped keyspace!!!");

		try
		{
			FileUtils.writeStringToFile(
					new File("output/CassandraCounterCreateEvaluationCase.txt"),
					builder.toString()
			);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
