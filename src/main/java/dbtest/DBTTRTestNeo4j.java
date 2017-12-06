package dbtest;

import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

import java.io.IOException;

import org.apache.commons.lang.time.StopWatch;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.neo4j.Neo4jDriver;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;


/**
 * Neo4j testsuite.
 * @author Manuel Stoeckel
 * Created on 6 Jun 2017
 */
public class DBTTRTestNeo4j {
	
	public static void main(String args[]) throws Exception {
		long begin = System.currentTimeMillis();
		init();

		System.out.println("Test took: " + (System.currentTimeMillis()-begin) + " ms.");
	}
	
	/**
	 * Initialize the database. db_dir must exist and be empty.
	 * @throws UIMAException -
	 * @throws IOException -
	 * @throws ResourceInitializationException -
	 */
	public static void init() throws ResourceInitializationException, UIMAException, IOException {
		CollectionReader reader = CollectionReaderFactory.createReader(XmiReaderModified.class,
				XmiReaderModified.PARAM_PATTERNS,"[+]**/???.xmi.gz",
				XmiReaderModified.PARAM_SOURCE_LOCATION,"/Users/peugeotbaguette/Downloads/biologie",
				XmiReaderModified.PARAM_LANGUAGE,"de");

		runPipeline(
				reader,
					DBWriterTest.getNeo4JWriter());
	}


@Deprecated
public static void testAll () throws Exception {
		System.out.println("Testing getTTR(List of 2):");
		long timeBegin = System.currentTimeMillis();
//		Neo4jQueryHandler.get_TTR(Arrays.asList("105","159"));
		System.out.println("getTTR(List of 2) took: " + (System.currentTimeMillis() - timeBegin) + "  ms\n");
		System.out.println("Testing getTTR():");
		timeBegin = System.currentTimeMillis();
//		Neo4jQueryHandler.get_TTR();
		System.out.println("getTTR() took: " + (System.currentTimeMillis() - timeBegin) + "  ms\n");
		System.out.println("Testing count(LEMMA):");
		timeBegin = System.currentTimeMillis();
//		Neo4jQueryHandler.count(Const.TYPE.LEMMA);
		System.out.println("count(LEMMA) took: " + (System.currentTimeMillis() - timeBegin) + "  ms\n");
		System.out.println("Testing count(DocId, TOKEN):");
		timeBegin = System.currentTimeMillis();
//		Neo4jQueryHandler.count("105",Const.TYPE.TOKEN);
		System.out.println("count(DocId, TOKEN) took: " + (System.currentTimeMillis() - timeBegin) + "  ms\n");
		System.out.println("Testing count(LEMMA, Mensch):");
		timeBegin = System.currentTimeMillis();
//		Neo4jQueryHandler.count(Const.TYPE.LEMMA, "Mensch");
		System.out.println("count(LEMMA, Mensch) took: " + (System.currentTimeMillis() - timeBegin) + " ms\n");
	}
	
	/**
	 * Deletes all Data from the database.
	 * @throws Exception -
	 */
	@Deprecated
	public static void deleteAll() throws Exception {
		Neo4jDriver driver = new Neo4jDriver ();
		try {
			System.out.println("Deleting all...");
			driver.runSimpleQuery("MATCH (n) DETACH DELETE n");
			System.out.println("All deleted.");
		} catch(Exception e) {
			throw e;
		} finally {
			driver.close();
		}
	}

	@Deprecated
	public static void testQueryTimes (int runs) throws Exception {
		StopWatch a = new StopWatch();
		a.start();
		a.suspend();
		StopWatch b = new StopWatch();
		b.start();
		b.suspend();
		for(int i=0;i<runs;i++) {
		    a.resume();
//		    Neo4jQueryHandler.get_Lemmata("1063");
		    a.suspend();
			
		    b.resume();
//		    Neo4jQueryHandler.getLemmata("1420");
			b.suspend();
		}
		System.out.println(a.getTime()+"|"+b.getTime());
	}
}