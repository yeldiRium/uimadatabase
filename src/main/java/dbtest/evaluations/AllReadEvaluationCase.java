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
import org.hucompute.services.uima.database.neo4j.Neo4jCollectionReader;
import org.hucompute.services.uima.database.neo4j.Neo4jCollectionReaderNew;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;

import java.io.File;
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
					getXMIReader(),
					// TODO: update this once the ConnectionResponse has been rewritten
					getNeo4jReader((Connection) connectionResponse.getConnections().stream().filter((connection) -> connection instanceof Neo4jConnection).toArray()[0]),
					getCassandraReader(),
					getMongoReader(),
					getXMIReader(),
					getMysqlReader()
			);
			for (CollectionReader reader : readers)
			{
				runPipeline(
						reader,
						createEngine(StatusPrinter.class)
						//				createEngine(XmiWriter.class, XmiWriter.PARAM_TARGET_LOCATION,"/home/ahemati/testDocuments/output",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
						//				createEngine(XmiWriter.class, XmiWriter.PARAM_TARGET_LOCATION,"/home/ahemati/testDocuments/output",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
						//				createEngine(XmiWriter.class, XmiWriter.PARAM_TARGET_LOCATION,"/home/voinea/testxmi",XmiWriter.PARAM_USE_DOCUMENT_ID,true,XmiWriter.PARAM_OVERWRITE,true)
				);
			}
		} catch (UIMAException | IOException e)
		{
			e.printStackTrace();
		}
	}

	public static CollectionReader getXMIReader()
			throws ResourceInitializationException
	{
		return CollectionReaderFactory.createReader(
				XmiReaderModified.class,
				XmiReaderModified.PARAM_SOURCE_LOCATION,
				System.getenv("INPUT_DIR"),
				XmiReaderModified.PARAM_PATTERNS,
				"[+]**/*.xmi.gz",
				XmiReaderModified.PARAM_LOG_FILE_LOCATION,
				new File("output/AllReadEvaluationCase_xmi.log"),
				XmiReaderModified.PARAM_LANGUAGE,
				"de"
		);
	}

	public static CollectionReader getMongoReader()
			throws ResourceInitializationException
	{
		return CollectionReaderFactory.createReader(
				MongoCollectionReader.class,
				MongoCollectionReader.PARAM_DB_CONNECTION,
				new String[]{"localhost", "test_with_index", "wikipedia", "", ""},
				MongoCollectionReader.PARAM_LOG_FILE_LOCATION,
				new File("output/AllReadEvaluationCase_mongo.log")
//			MongoCollectionReader.PARAM_QUERY,"{}",
		);
	}

	public static CollectionReader getMysqlReader()
			throws ResourceInitializationException
	{
		return CollectionReaderFactory.createReader(
				MysqlCollectionReader.class,
				MysqlCollectionReader.PARAM_LOG_FILE_LOCATION,
				new File("output/AllReadEvaluationCase_mysql.log")
		);
	}

	public static CollectionReader getNeo4jReader(Connection neo4jConnection)
			throws ResourceInitializationException
	{
		return CollectionReaderFactory.createReader(
				Neo4jCollectionReaderNew.class,
				Neo4jCollectionReaderNew.PARAM_LOG_FILE_LOCATION,
				new File("output/AllReadEvaluationCase_neo4j.log"),
				Neo4jCollectionReaderNew.PARAM_CONNECTION,
				neo4jConnection
		);
	}

	public static CollectionReader getBasexReader()
			throws ResourceInitializationException
	{
		return CollectionReaderFactory.createReader(
				BasexCollectionReader.class,
				BasexCollectionReader.PARAM_LOG_FILE_LOCATION,
				new File("output/AllReadEvaluationCase_basex.log")
		);
	}

	public static CollectionReader getCassandraReader()
			throws ResourceInitializationException
	{
		return CollectionReaderFactory.createReader(
				CassandraCollectionReader.class,
				CassandraCollectionReader.PARAM_LOG_FILE_LOCATION,
				new File("output/AllReadEvaluationCase_cassandra.log")
		);
	}
}
