package dbtest;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

import java.io.File;
import java.io.IOException;

import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.basex.BasexWriter;
import org.hucompute.services.uima.database.cassandra.CassandraWriter;
import org.hucompute.services.uima.database.mongo.MongoCollectionReader;
import org.hucompute.services.uima.database.mongo.MongoWriter;
import org.hucompute.services.uima.database.mysql.MysqlWriter;
import org.hucompute.services.uima.database.neo4j.Neo4jWriter;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;
import org.hucompute.services.uima.database.xmi.XmiWriterModified;


public class DBWriterTest {
	public static void main(String...args) throws UIMAException, IOException{

		CollectionReader reader = CollectionReaderFactory.createReader(XmiReaderModified.class,
				XmiReaderModified.PARAM_PATTERNS,"[+]**/???.xmi.gz", //
				XmiReaderModified.PARAM_SOURCE_LOCATION,"/Users/peugeotbaguette/Downloads/biologie",
				XmiReaderModified.PARAM_LANGUAGE,"de");

		runPipeline(
				reader,
//								getNeo4JWriter()
				//				getMongoWriter()
				getCassandraWriter()
				//				getBasexWriter()
				//				getMysqlWriter()
				//				getXMIWriter()
				);

	}

	public static AnalysisEngine getMongoWriter() throws ResourceInitializationException{
		return createEngine(
				MongoWriter.class, 
				MongoCollectionReader.PARAM_DB_CONNECTION, new String[]{"localhost","test_with_index","wikipedia","",""},
				MongoWriter.PARAM_LOG_FILE_LOCATION,new File("dbtest/mongo_with_index.log"));
	}

	public static AnalysisEngine getMysqlWriter() throws ResourceInitializationException{
		return createEngine(MysqlWriter.class);
	}


	public static AnalysisEngine getNeo4JWriter() throws ResourceInitializationException{
		return createEngine(Neo4jWriter.class,
				Neo4jWriter.PARAM_LOG_FILE_LOCATION,new File("dbtest/writer/neo4j.log"));
	}

	public static AnalysisEngine getBasexWriter() throws ResourceInitializationException{
		return createEngine(BasexWriter.class, BasexWriter.PARAM_LOG_FILE_LOCATION,new File("dbtest/writer/basex.log"));
	}

	public static AnalysisEngine getCassandraWriter() throws ResourceInitializationException{
		return createEngine(CassandraWriter.class, CassandraWriter.PARAM_LOG_FILE_LOCATION,new File("dbtest/writer/cassandra.log"));
	}

	public static AnalysisEngine getXMIWriter() throws ResourceInitializationException{
		return createEngine(
				XmiWriterModified.class, 
				XmiWriterModified.PARAM_TARGET_LOCATION,"/home/ahemati/testDocuments/output",
				XmiWriterModified.PARAM_USE_DOCUMENT_ID,true,
				XmiWriterModified.PARAM_OVERWRITE,true,
				XmiWriterModified.PARAM_LOG_FILE_LOCATION,new File("dbtest/writer/xmi.log")
				);
	}
}
