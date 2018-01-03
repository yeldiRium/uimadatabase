package dbtest.evaluations;

import dbtest.StatusPrinter;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.*;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.basex.BasexCollectionReader;
import org.hucompute.services.uima.database.cassandra.CassandraCollectionReader;
import org.hucompute.services.uima.database.mongo.MongoCollectionReader;
import org.hucompute.services.uima.database.mysql.MysqlCollectionReader;
import org.hucompute.services.uima.database.neo4j.Neo4jCollectionReaderNew;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

public class AllReadEvaluationCase implements EvaluationCase
{
	@Override
	public ConnectionRequest requestConnection()
	{
		// TODO: decide what to request here
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(MongoDBConnection.class);
		connectionRequest.addRequestedConnection(MySQLConnection.class);
		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		connectionRequest.addRequestedConnection(BaseXConnection.class);
		connectionRequest.addRequestedConnection(CassandraConnection.class);
		return connectionRequest;
	}

	/**
	 * Executes pipeline for each reader.
	 * TODO: remove XMI, add ArangoDB
	 *
	 * @param connectionResponse
	 */
	@Override
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	)
	{
		try
		{
			List<CollectionReader> readers = Arrays.asList(
					getXMIReader(outputProvider),
					getNeo4jReader(outputProvider, connectionResponse
							.getConnection(Neo4jConnection.class)),
					getCassandraReader(outputProvider),
					getMongoReader(outputProvider),
					getBasexReader(outputProvider),
					getMysqlReader(outputProvider)
			);
			for (CollectionReader reader : readers)
			{
				runPipeline(
						reader,
						createEngine(StatusPrinter.class)
						//				createEngine(XmiWriter.class,
						// XmiWriter.PARAM_TARGET_LOCATION,"/home/ahemati/testDocuments/output",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
						//				createEngine(XmiWriter.class,
						// XmiWriter.PARAM_TARGET_LOCATION,"/home/ahemati/testDocuments/output",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
						//				createEngine(XmiWriter.class,
						// XmiWriter.PARAM_TARGET_LOCATION,"/home/voinea/testxmi",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
				);
			}
		} catch (UIMAException | IOException e)
		{
			e.printStackTrace();
		}
	}

	public static CollectionReader getXMIReader(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return CollectionReaderFactory.createReader(
				XmiReaderModified.class,
				XmiReaderModified.PARAM_SOURCE_LOCATION,
				System.getenv("INPUT_DIR"),
				XmiReaderModified.PARAM_PATTERNS,
				"[+]**/*.xmi.gz",
				XmiReaderModified.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllReadEvaluationCase.class.getName
						(), "xmi"),
				XmiReaderModified.PARAM_LANGUAGE,
				"de"
		);
	}

	public static CollectionReader getMongoReader(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return CollectionReaderFactory.createReader(
				MongoCollectionReader.class,
				MongoCollectionReader.PARAM_DB_CONNECTION,
				new String[]{"localhost", "test_with_index", "wikipedia", "",
						""},
				MongoCollectionReader.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllReadEvaluationCase.class.getName
						(), "mongo")
//			MongoCollectionReader.PARAM_QUERY,"{}",
		);
	}

	public static CollectionReader getMysqlReader(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return CollectionReaderFactory.createReader(
				MysqlCollectionReader.class,
				MysqlCollectionReader.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllReadEvaluationCase.class.getName
						(), "mysql")
		);
	}

	public static CollectionReader getNeo4jReader(OutputProvider
			                                              outputProvider, Connection neo4jConnection)
			throws ResourceInitializationException, IOException
	{
		return CollectionReaderFactory.createReader(
				Neo4jCollectionReaderNew.class,
				Neo4jCollectionReaderNew.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllReadEvaluationCase.class.getName
						(), "neo4j"),
				Neo4jCollectionReaderNew.PARAM_CONNECTION,
				neo4jConnection
		);
	}

	public static CollectionReader getBasexReader(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return CollectionReaderFactory.createReader(
				BasexCollectionReader.class,
				BasexCollectionReader.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllReadEvaluationCase.class.getName
						(), "basex")
		);
	}

	public static CollectionReader getCassandraReader(OutputProvider
			                                                  outputProvider)
			throws ResourceInitializationException, IOException
	{
		return CollectionReaderFactory.createReader(
				CassandraCollectionReader.class,
				CassandraCollectionReader.PARAM_LOG_FILE_LOCATION,
				outputProvider.createFile(AllReadEvaluationCase.class.getName
						(), "cassandra")
		);
	}
}
