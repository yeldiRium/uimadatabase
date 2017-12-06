package dbtest;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

import java.io.File;
import java.io.IOException;

import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.basex.BasexCollectionReader;
import org.hucompute.services.uima.database.cassandra.CassandraCollectionReader;
import org.hucompute.services.uima.database.mongo.MongoCollectionReader;
import org.hucompute.services.uima.database.mysql.MysqlCollectionReader;
import org.hucompute.services.uima.database.neo4j.Neo4jCollectionReader;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;

public class DBReaderTest {

	public static void main(String[] args) throws IOException, UIMAException {
//		CollectionReader reader = getXMIReader();
//		CollectionReader reader = getNeo4jReader();
//		CollectionReader reader = getCassandraReader();

//		CollectionReader reader = getMongoReader();
//				CollectionReader reader = getXMIReader();
//				CollectionReader reader = getMysqlReader();
		runPipeline(
//				getNeo4jReader()
//				getXMIReader()
//				getBasexReader()
				getCassandraReader()
				,createEngine(StatusPrinter.class)
//				,createEngine(XmiWriter.class, XmiWriter.PARAM_TARGET_LOCATION,"/home/ahemati/testDocuments/output",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
//				,createEngine(XmiWriter.class, XmiWriter.PARAM_TARGET_LOCATION,"/home/ahemati/testDocuments/output",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
//				,createEngine(XmiWriter.class, XmiWriter.PARAM_TARGET_LOCATION,"/home/voinea/testxmi",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
				);
	}

	public static CollectionReader getXMIReader() throws ResourceInitializationException{
		return  CollectionReaderFactory.createReader(XmiReaderModified.class,
				XmiReaderModified.PARAM_SOURCE_LOCATION,"/home/ahemati/biologie",
				XmiReaderModified.PARAM_PATTERNS,"[+]**/*.xmi.gz",
				XmiReaderModified.PARAM_LOG_FILE_LOCATION, new File("dbtest/read/xmi.log"),
				XmiReaderModified.PARAM_LANGUAGE,"de");
	}

	public static CollectionReader getMongoReader() throws ResourceInitializationException{
		return  CollectionReaderFactory.createReader(MongoCollectionReader.class,
				MongoCollectionReader.PARAM_DB_CONNECTION, new String[]{"localhost","test_with_index","wikipedia","",""},
				MongoCollectionReader.PARAM_LOG_FILE_LOCATION, new File("dbtest/read/mongo.log")
				//				MongoCollectionReader.PARAM_QUERY,"{}",
				);
	}
	
	public static CollectionReader getMysqlReader() throws ResourceInitializationException{
		return  CollectionReaderFactory.createReader(MysqlCollectionReader.class,
				MysqlCollectionReader.PARAM_LOG_FILE_LOCATION, new File("dbtest/read/mysql.log")
				);
	}
	
	public static CollectionReader getNeo4jReader() throws ResourceInitializationException{
		return  CollectionReaderFactory.createReader(Neo4jCollectionReader.class,
				Neo4jCollectionReader.PARAM_LOG_FILE_LOCATION, new File("dbtest/read/neo4j.log")
				);
	}
	
	public static CollectionReader getBasexReader() throws ResourceInitializationException{
		return  CollectionReaderFactory.createReader(BasexCollectionReader.class,
				BasexCollectionReader.PARAM_LOG_FILE_LOCATION, new File("dbtest/read/basex.log")
				);
	}
	
	public static CollectionReader getCassandraReader() throws ResourceInitializationException{
		return  CollectionReaderFactory.createReader(CassandraCollectionReader.class,
				CassandraCollectionReader.PARAM_LOG_FILE_LOCATION, new File("dbtest/read/cassandra.log")
				);
	}
	
}
