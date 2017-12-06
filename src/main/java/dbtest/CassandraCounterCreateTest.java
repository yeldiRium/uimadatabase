package dbtest;

import java.io.IOException;
import java.util.HashMap;

import org.apache.uima.UIMAException;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.cassandra.CassandraIndexWriter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Tests all tables writes and table queries. 
 * @author Luis Glaser
 *
 */
public class CassandraCounterCreateTest {
	public static void main(String args[]) throws UIMAException, IOException, ResourceInitializationException {
	Cluster cluster = null;
	Session session = null;
	long start;
	long end;
	
	// Booleans for setting tests
    boolean lc = true;
    boolean dlc = true;
    boolean ttrW = true;
    boolean ttrR = true;
    boolean lo = true;
    boolean tf = true;
    boolean idf = true;
    boolean tfidf = true;
    boolean tfidfR = true;
    boolean DROPONE = false;
    boolean DROPTWO = false;
    int lcCount = 0;
    int dlcCount = 0;
    int ttrWCount = 0;
    int ttrRCount = 0;
    int loCount = 0;
    int tfCount = 0;
    int idfCount = 0;
    int tfidfCount = 0;
    int tfidfRCount = 0;
	try{
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("textimager");
		session.execute("use textimager;");
		
		if (lc){
		start = System.currentTimeMillis();
		CassandraIndexWriter.writeLemmaCounter(session);
		end = System.currentTimeMillis();
		lcCount=(int) (end-start);
		System.out.println("Creating lemmaCounter took: "+(end-start)+"ms.");
		}
		if(dlc){
		start = System.currentTimeMillis();
		CassandraIndexWriter.writeDistinctLemmaCounter(session);
		end = System.currentTimeMillis();
		dlcCount=(int) (end-start);
		System.out.println("Creating distinctLemmaCounter took: "+(end-start)+"ms.");
		}
		if (ttrW){
		start = System.currentTimeMillis();
		CassandraIndexWriter.writeTTR(session);
		end = System.currentTimeMillis();
		ttrWCount=(int) (end-start);
		System.out.println("Creating TTR took: "+(end-start)+"ms.");
		}
		if (ttrR){
		start = System.currentTimeMillis();
//		HashMap<String, Float> ttr = CassandraIndexWriter.getTTR(session);
//		System.out.println(ttr);
		end = System.currentTimeMillis();
		ttrRCount=(int) (end-start);
		System.out.println("Getting TTR took: "+(end-start)+"ms.");
		}
		if (lo){
			start = System.currentTimeMillis();
			CassandraIndexWriter.writeLemmaOccurrences(session);
			end = System.currentTimeMillis();
			loCount=(int) (end-start);
			System.out.println("Writing lemmaOccurences took: "+(end-start)+"ms.");
		}
		if (idf){
			start = System.currentTimeMillis();
			CassandraIndexWriter.writeIDF(session);
			end = System.currentTimeMillis();
			idfCount=(int) (end-start);
			System.out.println("Writing idf took: "+(end-start)+"ms.");
		}
		if (tf){
			start = System.currentTimeMillis();
			CassandraIndexWriter.writeFrequencyNormalizedByLength(session);
			end = System.currentTimeMillis();
			tfCount=(int) (end-start);
			System.out.println("Writing tf took: "+(end-start)+"ms.");
		}
		if (tfidf){
			start = System.currentTimeMillis();
			CassandraIndexWriter.writeTfIdf(session);
			end = System.currentTimeMillis();
			tfidfCount=(int) (end-start);
			System.out.println("Writing tf-idf took: "+(end-start)+"ms.");
		}
		if (tfidfR){
			start = System.currentTimeMillis();
//			CassandraIndexWriter.getTFIDF(session);
			end = System.currentTimeMillis();
			tfidfRCount=(int) (end-start);
			System.out.println("Reading tf-idf took: "+(end-start)+"ms.");
		}
		if (DROPONE&&DROPTWO){
			session.execute("drop keyspace textimager;");
			System.out.println("!!!Dropped keyspace!!!");
		}
		System.out.println("Testing Times\nlc = "+lcCount+"\ndlc = "+dlcCount+"\nttrW = "+ttrWCount+"\nttrR = "+ttrRCount+"\nlo = "+loCount+"\nidf = "+idfCount+"\ntf = "+tfCount+"\ntd-idf = "+tfidfCount+"\nGet td-idf = "+tfidfRCount);
	}catch (Exception e){
		e.printStackTrace();
	}finally{
		System.out.println("Shutting down session..");
		session.close();
		cluster.close();
	}
	
	
	}
}
