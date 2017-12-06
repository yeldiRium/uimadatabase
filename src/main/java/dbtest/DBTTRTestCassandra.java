package dbtest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class DBTTRTestCassandra {
	public static void main(String args[]) throws UIMAException, IOException, ResourceInitializationException {
		CollectionReader reader = CollectionReaderFactory.createReader(XmiReaderModified.class,
				XmiReaderModified.PARAM_PATTERNS,"[+]**/*.xmi", //
				XmiReaderModified.PARAM_SOURCE_LOCATION,"src/main/resources/testfiles",
				XmiReaderModified.PARAM_LANGUAGE,"de");



		long start;
		long end;
		Cluster cluster = null;
		Session session = null;
		ResultSet rs = null;
//		runPipeline(DBReaderTest.getCassandraReader(),createEngine(StatusPrinter.class));
		HashMap<String, Integer> lemmaCount = new HashMap<>();
		try{
			cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			session = cluster.connect("textimager");
			session.execute("use textimager;");
			
			session.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS lemmaView as select xmi, value from lemma "
				+ "where xmi is not null and value is not null and start is not null and end is not null "
				+ "primary key ((xmi, value), start, end) with clustering order by (xmi desc);");
//			start = System.currentTimeMillis();
//			
//			end = System.currentTimeMillis();
//			System.out.println("tfidf took: "+(end-start)+"ms.");
//			
			
			// Testing TTR
			start = System.currentTimeMillis();
//			HashMap<String, Double> ttr = CassandraQueryUtil.getTTR(session);
			end = System.currentTimeMillis();
			System.out.println("TTR took: "+(end-start)+"ms.");
			

			// Testing tfidf
			start = System.currentTimeMillis();
//			HashMap<String, Double> tfidf = CassandraQueryUtil.tfidf(session, "sein");
			end = System.currentTimeMillis();
			System.out.println("tfidf for one lemma took: "+(end-start)+"ms.");
			
			// Testing Tfidf for all lemmata in a testset with different textlengths,
			// horribly slow
			for (String xmi : new HashSet<String>(Arrays.asList("2372","44493","1402607","1656311","2911516"))){
				start = System.currentTimeMillis();
//				CassandraQueryUtil.getTfidfForDocument(session, xmi);
				end = System.currentTimeMillis();
				System.out.println(xmi+": "+(start-end)+"ms.");
			};

		
		}catch (Exception e){
			e.printStackTrace();
		}finally{
			System.out.println("Shutting down session..");
			session.close();
		}
		
		}

	
	static public HashMap<String, Double> calculateTTR(HashMap<String, Integer> tokens, HashMap<String, Integer> lemmata){
		HashMap<String, Double> ttr = new HashMap<>();
		for (String key : tokens.keySet()){
			ttr.put(key, (double)lemmata.get(key)/(double)tokens.get(key));
//			String key = e.getKey();
//			ret.put(key, (double)l.get(key)/(double)e.getValue());
		}
		return ttr;
	}
}