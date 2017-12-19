package dbtest.evaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationCase;
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

public class AllWriteEvaluationCase implements EvaluationCase
{
	@Override
	public ConnectionRequest requestConnection()
	{
		return null;
	}

	@Override
	public void run(ConnectionResponse connectionResponse)
	{
		try
		{
			CollectionReader reader = CollectionReaderFactory.createReader(
					XmiReaderModified.class,
					XmiReaderModified.PARAM_PATTERNS,
					"[+]**/???.xmi.gz", //
					XmiReaderModified.PARAM_SOURCE_LOCATION,
					"/Users/peugeotbaguette/Downloads/biologie",
					XmiReaderModified.PARAM_LANGUAGE,
					"de"
			);

			List<AnalysisEngine> writers = Arrays.asList(
					getNeo4JWriter(),
					getMongoWriter(),
					getCassandraWriter(),
					getBasexWriter(),
					getMysqlWriter(),
					getXMIWriter()
			);

			for (AnalysisEngine writer : writers)
			{
				try
				{
					runPipeline(
							reader,
							writer
					);
				} catch (UIMAException | IOException e)
				{
					// TODO: handle better
					e.printStackTrace();
				}
			}

		} catch (ResourceInitializationException e)
		{
			// TODO: handle better
			e.printStackTrace();
		}
	}

	public static AnalysisEngine getMongoWriter() throws
			ResourceInitializationException
	{
		return createEngine(
				MongoWriter.class,
				MongoCollectionReader.PARAM_DB_CONNECTION,
				new String[]{"localhost", "test_with_index", "wikipedia", "", ""},
				MongoWriter.PARAM_LOG_FILE_LOCATION,
				new File("output/AllWriteEvaluationCase_mongoWithIndex.log")
		);
	}

	public static AnalysisEngine getMysqlWriter()
			throws ResourceInitializationException
	{
		return createEngine(MysqlWriter.class);
	}


	public static AnalysisEngine getNeo4JWriter()
			throws ResourceInitializationException
	{
		return createEngine(Neo4jWriter.class,
				Neo4jWriter.PARAM_LOG_FILE_LOCATION,
				new File("output/AllWriteEvaluationCase_neo4j.log")
		);
	}

	public static AnalysisEngine getBasexWriter()
			throws ResourceInitializationException
	{
		return createEngine(
				BasexWriter.class,
				BasexWriter.PARAM_LOG_FILE_LOCATION,
				new File("output/AllWriteEvaluationCase_basex.log")
		);
	}

	public static AnalysisEngine getCassandraWriter()
			throws ResourceInitializationException
	{
		return createEngine(
				CassandraWriter.class,
				CassandraWriter.PARAM_LOG_FILE_LOCATION,
				new File("output/AllWriteEvaluationCase_cassandra.log")
		);
	}

	public static AnalysisEngine getXMIWriter()
			throws ResourceInitializationException
	{
		return createEngine(
				XmiWriterModified.class,
				XmiWriterModified.PARAM_TARGET_LOCATION,
				"/home/ahemati/testDocuments/output",
				XmiWriterModified.PARAM_USE_DOCUMENT_ID,
				true,
				XmiWriterModified.PARAM_OVERWRITE,
				true,
				XmiWriterModified.PARAM_LOG_FILE_LOCATION,
				new File("output/AllWriteEvaluationCase_xmi.log")
		);
	}
}
