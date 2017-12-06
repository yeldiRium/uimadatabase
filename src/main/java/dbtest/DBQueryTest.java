package dbtest;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.hucompute.services.uima.database.neo4j.data.Const;
import org.hucompute.services.uima.database.neo4j.impl.Document_Neo4J_Impl;
import org.hucompute.services.uima.database.neo4j.impl.Lemma_Neo4J_Impl;
import org.hucompute.services.uima.database.neo4j.impl.MDB_Neo4J_Impl;
import org.hucompute.services.uima.database.neo4j.impl.Pos_Neo4J_Impl;
import org.hucompute.services.uima.database.xmi.POSCounter;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.xml.sax.SAXException;

import com.google.common.collect.Iterators;

import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiReader;

public class DBQueryTest {
	public static void main(String...args) throws UIMAException, IOException, SAXException{

		//		getPosXMI();
//		   	getLemmaXMI();

//			getPosDistributionNeo4j();
//			getLemmaDistributionNeo4j();
//			getTypeTokenDistributionNeo4j();as
	}

	public static void getPosXMI() throws UIMAException, IOException{
		long start = System.currentTimeMillis();
		CollectionReader xmireader = CollectionReaderFactory.createReader(XmiReaderModified.class,
				XmiReader.PARAM_SOURCE_LOCATION,"/media/ahemati/cea5347d-36d3-4856-a9be-bcd0bddbfd92/wikipedia_kategorien_sample/biologie",
				//						XmiReader.PARAM_SOURCE_LOCATION,"/home/ahemati/workspace/services/services-io/src/main/resources/1",
				XmiReader.PARAM_PATTERNS,"[+]**/*.xmi.gz",
				//		XmiReader.PARAM_PATTERNS,"/949491.xmi",
				XmiReader.PARAM_LANGUAGE,"de");

		runPipeline(
				xmireader,
				createEngine(POSCounter.class));
		FileUtils.writeStringToFile(new File("dbtest/query/xmi_sum_pos.log"), ""+(System.currentTimeMillis()-start));
		System.out.println(System.currentTimeMillis()-start);
	}
	
	public static void getLemmaXMI() throws UIMAException, IOException{
		long start = System.currentTimeMillis();
		CollectionReader xmireader = CollectionReaderFactory.createReader(XmiReaderModified.class,
				XmiReader.PARAM_SOURCE_LOCATION,"/home/ahemati/biologie",
				//						XmiReader.PARAM_SOURCE_LOCATION,"/home/ahemati/workspace/services/services-io/src/main/resources/1",
				XmiReader.PARAM_PATTERNS,"[+]**/*.xmi.gz",
				//		XmiReader.PARAM_PATTERNS,"/949491.xmi",
				XmiReader.PARAM_LANGUAGE,"de");

		runPipeline(
				xmireader,
				createEngine(POSCounter.class));
		FileUtils.writeStringToFile(new File("dbtest/query/xmi_sum_lemma.log"), ""+(System.currentTimeMillis()-start));
		System.out.println(System.currentTimeMillis()-start);
	}

	public static void getPosDistributionNeo4j() throws UIMAException, IOException{
		long start = System.currentTimeMillis();

		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl("/home/ahemati/workspace/XMI4Neo4J/conf.conf");

		Iterator<Node> pos = pMDB.getNodes(Pos_Neo4J_Impl.getLabel()).iterator();
		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx()) {
			pos.forEachRemaining(n->{
				System.out.println(n.getProperty("value"));
				System.out.println(Iterators.size(n.getRelationships(Direction.INCOMING, Const.RelationType.pos).iterator()));
			});
		}
		FileUtils.writeStringToFile(new File("dbtest/query/neo4j_sum_pos.log"), ""+(System.currentTimeMillis()-start));
		System.out.println(System.currentTimeMillis()-start);
	}
	
	static int i = 0;
	public static void getLemmaDistributionNeo4j() throws UIMAException, IOException{
		long start = System.currentTimeMillis();

		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl("/home/ahemati/workspace/XMI4Neo4J/conf.conf");

		Iterator<Node> pos = pMDB.getNodes(Lemma_Neo4J_Impl.getLabel()).iterator();
		
		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx()) {
			
			pos.forEachRemaining(n->{
				if(i++%1000==0)
					System.out.println(i);
				Iterators.size(n.getRelationships(Direction.INCOMING, Const.RelationType.lemma).iterator());
			});
			System.out.println(i);
		}
		FileUtils.writeStringToFile(new File("dbtest/query/neo4j_sum_lemma.log"), ""+(System.currentTimeMillis()-start));
		System.out.println(System.currentTimeMillis()-start);
	}
	
	public static void getLemmaDistributionBaseX() throws UIMAException, IOException{
		long start = System.currentTimeMillis();

		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl("/home/ahemati/workspace/XMI4Neo4J/conf.conf");

		Iterator<Node> pos = pMDB.getNodes(Lemma_Neo4J_Impl.getLabel()).iterator();
		
		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx()) {
			
			pos.forEachRemaining(n->{
				if(i++%1000==0)
					System.out.println(i);
				Iterators.size(n.getRelationships(Direction.INCOMING, Const.RelationType.lemma).iterator());
			});
			System.out.println(i);
		}
		FileUtils.writeStringToFile(new File("dbtest/query/neo4j_sum_lemma.log"), ""+(System.currentTimeMillis()-start));
		System.out.println(System.currentTimeMillis()-start);
	}
	
//	public static void getTypeTokenDistributionNeo4j() throws UIMAException, IOException{
//		long start = System.currentTimeMillis();
//
//		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl("/home/ahemati/workspace/XMI4Neo4J/conf.conf");
//
//		Iterator<Node> documents = pMDB.getNodes(Document_Neo4J_Impl.getLabel()).iterator();
//		
//		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx()) {
//			
//			documents.forEachRemaining(n->{
//				
////				System.out.println(n);
////				if(i++%1000==0)
////					System.out.println(i);
////				Iterators.size(n.getRelationships(Direction.INCOMING, Const.RelationType.lemma).iterator());
//			});
//			System.out.println(i);
//		}
//		FileUtils.writeStringToFile(new File("dbtest/query/neo4j_type_token.log"), ""+(System.currentTimeMillis()-start));
//		System.out.println(System.currentTimeMillis()-start);
//	}
}
