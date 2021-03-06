package org.hucompute.services.uima.eval.evaluation.implementation.collectionWriter;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.eval.database.abstraction.QueryHandlerInterface;
import org.hucompute.services.uima.eval.database.abstraction.implementation.BenchmarkQueryHandler;
import org.hucompute.services.uima.eval.database.connection.*;
import org.hucompute.services.uima.eval.utility.Formatting;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LongSummaryStatistics;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class EvaluatingCollectionWriter extends JCasConsumer_ImplBase
{
	protected static final Logger logger =
			Logger.getLogger(EvaluatingCollectionWriter.class.getName());

	// UIMA Parameters
	public static final String PARAM_OUTPUT_FILE = "outputFile";
	@ConfigurationParameter(name = PARAM_OUTPUT_FILE, mandatory = false)
	protected File outputFile;

	public static final String PARAM_DBNAME = "dbName";
	@ConfigurationParameter(name = PARAM_DBNAME)
	protected String dbName;

	protected BenchmarkQueryHandler queryHandler;
	protected JSONArray specificDocumentStatistics;
	protected int currentIndex;

	@Override
	public void initialize(UimaContext context)
			throws ResourceInitializationException
	{
		super.initialize(context);

		this.dbName = context.getConfigParameterValue(PARAM_DBNAME).toString();
		logger.info("Initializing CollectionWriter for db " + this.dbName
				+ ".");

		this.specificDocumentStatistics = new JSONArray();
		this.currentIndex = 0;

		Class<? extends Connection> connectionClass =
				Connections.getConnectionClassForName(this.dbName);
		ConnectionRequest request = new ConnectionRequest();
		request.addRequestedConnection(connectionClass);
		try
		{
			ConnectionResponse response = ConnectionManager.getInstance()
					.submitRequest(request).get();
			Connection connection = response
					.getConnection(connectionClass);

			// Set up Database before writing, since writing is always the first
			// step when evaluating.
			this.queryHandler = new BenchmarkQueryHandler(QueryHandlerInterface
					.createQueryHandlerForConnection(connection));
			this.queryHandler.setUpDatabase();
			this.queryHandler.openDatabase();

			logger.info("Clearing database for " + this.dbName + ".");
			this.queryHandler.clearDatabase();
		} catch (InterruptedException | ExecutionException e)
		{
			logger.severe("Initialization for CollectionWriter failed. " +
					"Interrupted when requesting connection for "
					+ this.dbName + ".");
			e.printStackTrace();
			Thread.currentThread().interrupt();
		} catch (IOException e)
		{
			logger.severe("Initialization for CollectionWriter failed. " +
					"Exception occured when clearing database for "
					+ this.dbName + ".");
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}

		logger.info("Initialized CollectionWriter for db " + this.dbName
				+ ".");
	}

	/**
	 * Iterates over the jCas' structure and inserts all relevant elements into
	 * the database.
	 *
	 * @param jCas The document to be processed.
	 * @throws AnalysisEngineProcessException If anything goes wrong at all.
	 */
	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException
	{
		this.currentIndex++;
		final String documentId = DocumentMetaData.get(jCas)
				.getDocumentId();

		logger.info(this.currentIndex + " Storing jCas \"" + documentId
				+ "\" into " + this.dbName + "...");
		long start = System.currentTimeMillis();

		this.queryHandler.storeDocumentHierarchy(jCas);

		long end = System.currentTimeMillis();
		long fullInsertTime = end - start;

		logger.info("JCas \"" + documentId + "\" processed and stored.");
		logger.info("Took " + fullInsertTime + "ms.");
	}

	/**
	 * Logs and writes all statistics to output.
	 * This is called in a pipeline after all documents have been processed.
	 */
	@Override
	public void collectionProcessComplete()
	{
		logger.info("Collection process complete. Statistics:");

		LongSummaryStatistics documentInsertStatistic = this.queryHandler
				.getMethodBenchmarks().get("storeDocumentHierarchy")
				.getCallTimes()
				.stream()
				.collect(
						Collectors.summarizingLong(Long::longValue)
				);

		// Format statistics as strings for logging and user readable output.
		String statistics = "Inserted " + documentInsertStatistic.getCount() + " documents.\n" +
				"  Inserting a complete document structure took " + Math.floor(documentInsertStatistic.getAverage()) + "ms on average.\n" +
				"  Inserting a document took at most " + documentInsertStatistic.getMax() + "ms.\n" +
				"  Spent " + documentInsertStatistic.getSum() + "ms overall on inserting documents.\n";

		logger.info(statistics);

		// Format statistics as JSON for output files for easier processing
		// later on.
		JSONObject statisticsJSON = new JSONObject();
		statisticsJSON.put(
				"hierarchy",
				Formatting.createOutputForMethod(
						"storeDocumentHierarchy", queryHandler
				)
		);

		try (BufferedWriter output =
				     new BufferedWriter(new FileWriter(this.outputFile))
		)
		{
			output.write(statisticsJSON.toString());
		} catch (IOException e)
		{
			// TODO: improve error handling
			logger.severe("Was not able to write statistics to file.");
			e.printStackTrace();
		}
	}
}
