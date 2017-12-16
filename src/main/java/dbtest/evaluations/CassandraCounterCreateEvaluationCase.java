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

			int lcCount = 0;
			int dlcCount = 0;
			int ttrWCount = 0;
			int ttrRCount = 0;
			int loCount = 0;
			int tfCount = 0;
			int idfCount = 0;
			int tfidfCount = 0;
			int tfidfRCount = 0;

			session.execute("create keyspace if not exists textimager with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
			session.execute("use textimager;");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeLemmaCounter(session);
			end = System.currentTimeMillis();
			lcCount=(int) (end-start);
			output.write("Creating lemmaCounter took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeDistinctLemmaCounter(session);
			end = System.currentTimeMillis();
			dlcCount=(int) (end-start);
			output.write("Creating distinctLemmaCounter took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeTTR(session);
			end = System.currentTimeMillis();
			ttrWCount=(int) (end-start);
			output.write("Creating TTR took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
//			HashMap<String, Float> ttr = CassandraIndexWriter.getTTR(session);
//			output.write();(ttr);
			end = System.currentTimeMillis();
			ttrRCount=(int) (end-start);
			output.write("Getting TTR took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeLemmaOccurrences(session);
			end = System.currentTimeMillis();
			loCount=(int) (end-start);
			output.write("Writing lemmaOccurences took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeIDF(session);
			end = System.currentTimeMillis();
			idfCount=(int) (end-start);
			output.write("Writing idf took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeFrequencyNormalizedByLength(session);
			end = System.currentTimeMillis();
			tfCount=(int) (end-start);
			output.write("Writing tf took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
			CassandraIndexWriter.writeTfIdf(session);
			end = System.currentTimeMillis();
			tfidfCount=(int) (end-start);
			output.write("Writing tf-idf took: "+(end-start)+"ms.");

			start = System.currentTimeMillis();
//			CassandraIndexWriter.getTFIDF(session);
			end = System.currentTimeMillis();
			tfidfRCount=(int) (end-start);
			output.write("Reading tf-idf took: "+(end-start)+"ms.");

			session.execute("drop keyspace textimager;");
			output.write("!!!Dropped keyspace!!!");

			output.write("Testing Times\nlc = "+lcCount+"\ndlc = "+dlcCount+"\nttrW = "+ttrWCount+"\nttrR = "+ttrRCount+"\nlo = "+loCount+"\nidf = "+idfCount+"\ntf = "+tfCount+"\ntd-idf = "+tfidfCount+"\nGet td-idf = "+tfidfRCount);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
