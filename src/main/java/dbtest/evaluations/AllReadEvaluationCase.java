package dbtest.evaluations;

import dbtest.StatusPrinter;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.Connections;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import dbtest.evaluations.collectionReader.EvaluatingCollectionReader;
import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiReader;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;

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
		return new ConnectionRequest();
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
					//getXMIReader(outputProvider),
					createReader(outputProvider, Connections.DBName.ArangoDB),
					createReader(outputProvider, Connections.DBName.Neo4j)
					//getCassandraReader(outputProvider),
					//getMongoReader(outputProvider),
					//getBasexReader(outputProvider),
					//getMysqlReader(outputProvider)
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
				XmiReader.class,
				XmiReader.PARAM_SOURCE_LOCATION,
				System.getenv("INPUT_DIR"),
				XmiReader.PARAM_PATTERNS,
				"[+]*.xmi.gz",
				XmiReader.PARAM_LANGUAGE,
				"de"
		);
	}

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
