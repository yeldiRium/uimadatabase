package dbtest.evaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
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
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	)
	{
		try
		{
			CollectionReader reader = CollectionReaderFactory.createReader(
					XmiReaderModified.class,
					XmiReaderModified.PARAM_PATTERNS,
					"[+]**/???.xmi.gz", //
					XmiReaderModified.PARAM_SOURCE_LOCATION,
					System.getenv("INPUT_DIR"),
					XmiReaderModified.PARAM_LANGUAGE,
					"de"
			);

			List<AnalysisEngine> writers = Arrays.asList(
					getNeo4JWriter(outputProvider),
					getMongoWriter(outputProvider),
					getCassandraWriter(outputProvider),
					getBasexWriter(outputProvider),
					getMysqlWriter(outputProvider),
					getXMIWriter(outputProvider)
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
		} catch (IOException e)
		{
			// TODO: handle better. This occurs, if an output file could not be created or sth.
			e.printStackTrace();
		}
	}

	public static AnalysisEngine getMongoWriter(OutputProvider outputProvider) throws
			ResourceInitializationException, IOException
	{
		return createEngine(
				MongoWriter.class,
				MongoCollectionReader.PARAM_DB_CONNECTION,
				new String[]{"localhost", "test_with_index", "wikipedia", "", ""},
				MongoWriter.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllWriteEvaluationCase.class.getName(), "mongoWithIndex")
		);
	}

	public static AnalysisEngine getMysqlWriter(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return createEngine(
				MysqlWriter.class,
				MongoWriter.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllWriteEvaluationCase.class.getName(), "mysqlWithIndex")
		);
	}


	public static AnalysisEngine getNeo4JWriter(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return createEngine(Neo4jWriter.class,
				Neo4jWriter.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllWriteEvaluationCase.class.getName(), "neo4j")
		);
	}

	public static AnalysisEngine getBasexWriter(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return createEngine(
				BasexWriter.class,
				BasexWriter.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllWriteEvaluationCase.class.getName(), "basex")
		);
	}

	public static AnalysisEngine getCassandraWriter(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return createEngine(
				CassandraWriter.class,
				CassandraWriter.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllWriteEvaluationCase.class.getName(), "cassandra")
		);
	}

	public static AnalysisEngine getXMIWriter(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return createEngine(
				XmiWriterModified.class,
				XmiWriterModified.PARAM_TARGET_LOCATION,
				System.getenv("OUTPUT_DIR"),
				XmiWriterModified.PARAM_USE_DOCUMENT_ID,
				true,
				XmiWriterModified.PARAM_OVERWRITE,
				true,
				XmiWriterModified.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllWriteEvaluationCase.class.getName(), "xmi")
		);
	}
}
