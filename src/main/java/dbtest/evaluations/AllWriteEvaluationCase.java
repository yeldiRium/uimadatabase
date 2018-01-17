package dbtest.evaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.Connections;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import dbtest.evaluations.collectionWriter.EvaluatingCollectionWriter;
import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiReader;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

/**
 * Evaluates the times for writing full documents to databases.
 * Creates instances of AnalysisEngine from specific CollectionWriters which
 * benchmark and document the time it takes to write full Documents and their
 * respective structures to the databases and write results into here defined
 * output files.
 * <p>
 * Tests all store* methods on QueryHandlerInterface implementations.
 */
public class AllWriteEvaluationCase implements EvaluationCase
{
	@Override
	public ConnectionRequest requestConnection()
	{
		// Since it is impossible to inject non-primitive objects into Analysis-
		// Engines, we don't need any connections here.
		// Instead we'll use the Singleton ConnectionManager in each Writer
		// and retrieve the Connections there.
		return new ConnectionRequest();
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
					XmiReader.class,
					XmiReader.PARAM_PATTERNS,
					"[+]*.xmi.gz", //
					XmiReader.PARAM_SOURCE_LOCATION,
					System.getenv("INPUT_DIR"),
					XmiReader.PARAM_LANGUAGE,
					"de"
			);

			List<AnalysisEngine> writers = Arrays.asList(
					//createWriter(outputProvider, Connections.DBName.ArangoDB),
					//createWriter(outputProvider, Connections.DBName.BaseX),
					//createWriter(outputProvider, Connections.DBName.Cassandra),
					//createWriter(outputProvider, Connections.DBName.MongoDB),
					//createWriter(outputProvider, Connections.DBName.MySQL),
					createWriter(outputProvider, Connections.DBName.Neo4j)
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

	/**
	 * Creates an AnalysisEngine for the UIMA pipeline for a given dbName.
	 * Initializes the CollectionWriter with a different outputFile for each
	 * database.
	 *
	 * @param outputProvider The outputProvider, which creates the output files.
	 * @param dbName         The name of the database for which a writer should
	 *                       be created.
	 * @return The initialized AnalysisEngine containing a CollectionWriter.
	 * @throws IOException                     If the output file can not be
	 *                                         created.
	 * @throws ResourceInitializationException If something inside UIMA went
	 *                                         wrong.
	 */
	public static AnalysisEngine createWriter(
			OutputProvider outputProvider, Connections.DBName dbName
	) throws IOException, ResourceInitializationException
	{
		return createEngine(
				EvaluatingCollectionWriter.class,
				EvaluatingCollectionWriter.PARAM_DBNAME,
				dbName.toString(),
				EvaluatingCollectionWriter.PARAM_OUTPUT_FILE,
				outputProvider.createFile(
						AllWriteEvaluationCase.class.getName(),
						dbName.toString()
				)
		);
	}
}
