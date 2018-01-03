package dbtest.evaluations;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.CassandraConnection;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import org.apache.commons.io.FileUtils;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class CassandraTTREvaluationCase implements EvaluationCase
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
		Collection<Connection> connections = connectionResponse.getConnections();
		CassandraConnection cassandraConnection = (CassandraConnection) connections.iterator().next();
		try
		{
			CollectionReader reader = CollectionReaderFactory.createReader(
					XmiReaderModified.class,
					XmiReaderModified.PARAM_PATTERNS,
					"[+]**/*.xmi",
					XmiReaderModified.PARAM_SOURCE_LOCATION,
					"src/main/resources/testfiles",
					XmiReaderModified.PARAM_LANGUAGE,
					"de"
			);

			long start;
			long end;
			Session session = cassandraConnection.getSession();
			session.execute("create keyspace if not exists textimager with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
			session.execute("use textimager;");
			StringBuilder builder = new StringBuilder();

			ResultSet rs = null;
			HashMap<String, Integer> lemmaCount = new HashMap<>();
			session.execute("use textimager;");

			session.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS lemmaView as select xmi, value from lemma "
					+ "where xmi is not null and value is not null and start is not null and end is not null "
					+ "primary key ((xmi, value), start, end) with clustering order by (xmi desc);");

			// Testing TTR
			start = System.currentTimeMillis();
			// TODO: use CassandraQueryHandler
//			HashMap<String, Double> ttr = CassandraQueryUtil.getTTR(session);
			end = System.currentTimeMillis();
			builder.append("TTR took: ")
					.append(end - start)
					.append("ms.\n");


			// Testing tfidf
			start = System.currentTimeMillis();
			// TODO: use CassandraQueryHandler
//			HashMap<String, Double> tfidf = CassandraQueryUtil.tfidf(session, "sein");
			end = System.currentTimeMillis();
			builder.append("tfidf for one lemma took: ")
					.append(end - start)
					.append("ms.\n");

			// Testing Tfidf for all lemmata in a testset with different textlengths,
			// horribly slow
			for (String xmi : new HashSet<String>(Arrays.asList("2372", "44493", "1402607", "1656311", "2911516")))
			{
				start = System.currentTimeMillis();
				// TODO: use CassandraQueryHandler
				// CassandraQueryUtil.getTfidfForDocument(session, xmi);
				end = System.currentTimeMillis();
				builder.append(xmi)
						.append(": ")
						.append(start - end)
						.append("ms.\n");
			}
			session.close();
			FileUtils.writeStringToFile(
					outputProvider.createFile(CassandraTTREvaluationCase.class.getName(), "index"),
					builder.toString()
			);
		} catch (ResourceInitializationException | IOException e)
		{
			e.printStackTrace();
		}
	}

	// TODO: is this method necessary?
	static public HashMap<String, Double> calculateTTR(HashMap<String, Integer> tokens, HashMap<String, Integer> lemmata)
	{
		HashMap<String, Double> ttr = new HashMap<>();
		for (String key : tokens.keySet())
		{
			ttr.put(key, (double) lemmata.get(key) / (double) tokens.get(key));
//			String key = e.getKey();
//			ret.put(key, (double)l.get(key)/(double)e.getValue());
		}
		return ttr;
	}
}
