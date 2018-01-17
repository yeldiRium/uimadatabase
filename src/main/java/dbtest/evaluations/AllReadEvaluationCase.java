package dbtest.evaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.Connections;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import dbtest.evaluations.collectionReader.EvaluatingCollectionReader;
import dbtest.evaluations.collectionWriter.IdleCollectionWriter;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

/**
 * Evaluates the times for reading out full documents from databases.
 * Creates instances of specific CollectionReaders which document and benchmark
 * readtimes and write results into here defined output files.
 * <p>
 * Tests the populateCasWithDocument method on QueryHandlerInterface implementa-
 * tions.
 */
public class AllReadEvaluationCase implements EvaluationCase
{
	@Override
	public ConnectionRequest requestConnection()
	{
		// CollectionReader construction follows the structure of the
		// CollectionWriters in the AllWriteEvaluationCase for simplicity. It is
		// possible to inject the Connections from here, but a similar structure
		// in both EvaluationCases is preferred.
		// See AllWriteEvaluationCase for elaboration on this.
		return new ConnectionRequest();
	}

	/**
	 * Creates and executes an EvaluatingCollectionWriter for each database.
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
					//createReader(outputProvider, Connections.DBName.ArangoDB),
					//createReader(outputProvider, Connections.DBName.BaseX),
					//createReader(outputProvider, Connections.DBName.Cassandra),
					//createReader(outputProvider, Connections.DBName.MongoDB),
					//createReader(outputProvider, Connections.DBName.MySQL),
					createReader(outputProvider, Connections.DBName.Neo4j)
			);
			for (CollectionReader reader : readers)
			{
				runPipeline(
						reader,
						// We don't need to process anything, since we only want
						// to benchmark the reading process. So the
						// AnalysisEngine here won't do anything.
						createEngine(IdleCollectionWriter.class)
				);
			}
		} catch (UIMAException | IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Creates a CollectionReader for the UIMA pipeline for a given dbName.
	 * Initializes the reader with a different outputFile for each database.
	 *
	 * @param outputProvider The outputProvider, which creates the output files.
	 * @param dbName         The name of the database for which a reader should
	 *                       be created.
	 * @return The initialized CollectionReader
	 * @throws IOException                     If the output file can not be
	 *                                         created.
	 * @throws ResourceInitializationException If something inside UIMA went
	 *                                         wrong.
	 */
	public static CollectionReader createReader(
			OutputProvider outputProvider,
			Connections.DBName dbName
	) throws IOException, ResourceInitializationException
	{
		return CollectionReaderFactory.createReader(
				EvaluatingCollectionReader.class,
				EvaluatingCollectionReader.PARAM_DBNAME,
				dbName.toString(),
				EvaluatingCollectionReader.PARAM_OUTPUT_FILE,
				outputProvider.createFile(
						AllReadEvaluationCase.class.getName(),
						dbName.toString()
				)
		);
	}
}
