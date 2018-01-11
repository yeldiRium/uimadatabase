package dbtest.evaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.Connections;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import dbtest.evaluations.collectionWriter.EvaluatingCollectionWriter;
import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiReader;
import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiWriter;
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
					//getMongoWriter(outputProvider),
					//getCassandraWriter(outputProvider),
					//getBasexWriter(outputProvider),
					//getMysqlWriter(outputProvider),
					//createWriter(outputProvider, Connections.DBName.Neo4j)
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

	public static AnalysisEngine getXMIWriter(OutputProvider outputProvider)
			throws ResourceInitializationException, IOException
	{
		return createEngine(
				XmiWriter.class,
				XmiWriter.PARAM_TARGET_LOCATION,
				System.getenv("OUTPUT_DIR"),
				XmiWriter.PARAM_USE_DOCUMENT_ID,
				true,
				XmiWriter.PARAM_OVERWRITE,
				true
		);
	}
}
