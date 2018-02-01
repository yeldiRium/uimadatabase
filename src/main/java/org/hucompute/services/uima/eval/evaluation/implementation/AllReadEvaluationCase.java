package org.hucompute.services.uima.eval.evaluation.implementation;

import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.hucompute.services.uima.eval.evaluation.implementation.collectionReader.EvaluatingCollectionReader;
import org.hucompute.services.uima.eval.evaluation.implementation.collectionWriter.IdleCollectionWriter;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.hucompute.services.uima.eval.evaluation.implementation.collectionReader.EvaluatingCollectionReader;
import org.hucompute.services.uima.eval.evaluation.implementation.collectionWriter.IdleCollectionWriter;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;

import java.io.IOException;
import java.util.logging.Logger;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

/**
 * Evaluates the times for reading out full documents from databases.
 * Creates instances of specific CollectionReaders which document and benchmark
 * read times and write results into here defined output files.
 * <p>
 * Tests the populateCasWithDocument method on QueryHandlerInterface implementa-
 * tions.
 */
public class AllReadEvaluationCase implements EvaluationCase
{
	protected static final Logger logger =
			Logger.getLogger(AllReadEvaluationCase.class.getName());

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

	@Override
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	)
	{
		for (Connections.DBName dbName : new Connections.DBName[]{
				Connections.DBName.ArangoDB,
				Connections.DBName.BaseX,
				Connections.DBName.Neo4j
		})
		{
			logger.info("Starting AllReadEvaluationCase for Database \""
					+ dbName + "\".");

			try
			{
				EvaluatingCollectionReader reader =
						(EvaluatingCollectionReader) createReader(
								outputProvider, dbName
						);
				runPipeline(
						reader,
						// We don't need to process anything, since we only want
						// to benchmark the reading process. So the
						// AnalysisEngine here won't do anything.
						AnalysisEngineFactory.createEngine(IdleCollectionWriter.class)
				);
				reader.writeOutput();
			} catch (UIMAException | IOException e)
			{
				logger.severe("AllReadEvaluationCase for Database \""
						+ dbName + "\" crashed.");
				// TODO: handle better
				e.printStackTrace();
			}

			logger.info("AllReadEvaluationCase for Database \""
					+ dbName + "\" done.");
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
