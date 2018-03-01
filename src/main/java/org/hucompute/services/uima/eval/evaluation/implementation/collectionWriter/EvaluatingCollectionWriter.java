package org.hucompute.services.uima.eval.evaluation.implementation.collectionWriter;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
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
			connection.getQueryHandler().setUpDatabase();

			this.queryHandler = new BenchmarkQueryHandler(
					connection.getQueryHandler()
			);
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
		try
		{
			this.queryHandler.storeJCasDocument(jCas);
		} catch (QHException e)
		{
			logger.severe("There was an error when trying to insert "
					+ documentId + ".");
			e.getException().printStackTrace();
			return;
		}

		int paragraphCount = 0;
		int sentenceCount = 0;
		int tokenCount = 0;
		JSONObject specificDocumentStatistic = new JSONObject();

		try
		{
			/*
			 * Store each element of the jCas that was annotated as a Para-
			 * graph.
			 */
			Paragraph previousParagraph = null;
			for (Paragraph paragraph
					: JCasUtil.select(jCas, Paragraph.class))
			{
				this.queryHandler.storeParagraph(
						paragraph, jCas, previousParagraph
				);
				paragraphCount++;
				previousParagraph = paragraph;

				/*
				 * Store each element of the jCas that was annotated as a Sen-
				 * tence and is contained in the current paragraph.
				 */
				Sentence previousSentence = null;
				for (Sentence sentence : JCasUtil.selectCovered(
						jCas,
						Sentence.class, paragraph
				))
				{
					this.queryHandler.storeSentence(
							sentence, jCas, paragraph, previousSentence
					);
					sentenceCount++;
					previousSentence = sentence;


					/*
					 * Store each element of the jCas that was annotated as a
					 * Token and is contained in the current sentence.
					 */
					Token previousToken = null;
					for (Token token : JCasUtil.selectCovered(
							jCas, Token.class, sentence
					))
					{
						this.queryHandler.storeToken(
								token, jCas, paragraph, sentence, previousToken
						);
						tokenCount++;
						previousToken = token;
					}
				}
			}
		} catch (UnsupportedOperationException e)
		{
			specificDocumentStatistic.put(
					"error", "An insert method was not supported."
			);
			specificDocumentStatistic.put(
					"stackTrace", e.getStackTrace()
			);
		}
		long end = System.currentTimeMillis();
		long fullInsertTime = end - start;

		specificDocumentStatistic.put(
				"documentId", documentId
		);
		specificDocumentStatistic.put(
				"paragraphs", paragraphCount
		);
		specificDocumentStatistic.put(
				"sentences", sentenceCount
		);
		specificDocumentStatistic.put(
				"tokens", tokenCount
		);
		specificDocumentStatistic.put(
				"fullInsertTime", fullInsertTime
		);
		specificDocumentStatistic.put(
				"textLength", jCas.getDocumentText().length()
		);
		this.specificDocumentStatistics.put(specificDocumentStatistic);

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
				.getMethodBenchmarks().get("storeJCasDocument")
				.getCallTimes()
				.stream()
				.collect(
						Collectors.summarizingLong(Long::longValue)
				);

		LongSummaryStatistics paragraphInsertStatistic = this.queryHandler
				.getMethodBenchmarks().get("storeParagraph")
				.getCallTimes()
				.stream()
				.collect(
						Collectors.summarizingLong(Long::longValue)
				);

		LongSummaryStatistics sentenceInsertStatistic = this.queryHandler
				.getMethodBenchmarks().get("storeSentence")
				.getCallTimes()
				.stream()
				.collect(
						Collectors.summarizingLong(Long::longValue)
				);

		LongSummaryStatistics tokenInsertStatistic = this.queryHandler
				.getMethodBenchmarks().get("storeToken")
				.getCallTimes()
				.stream()
				.collect(
						Collectors.summarizingLong(Long::longValue)
				);

		int averageDocumentStructureInsertTime = (int)
				(documentInsertStatistic.getSum()
						+ paragraphInsertStatistic.getSum()
						+ sentenceInsertStatistic.getSum()
						+ tokenInsertStatistic.getSum()
						/ (double) documentInsertStatistic.getCount());

		// Format statistics as strings for logging and user readable output.
		String statistics = "Inserted " + documentInsertStatistic.getCount() + " documents.\n" +
				"  Inserting a complete document structure took " + averageDocumentStructureInsertTime + "ms on average.\n" +
				"Inserted " + paragraphInsertStatistic.getCount() + " paragraphs.\n" +
				"  Inserting a paragraph took " + (int) paragraphInsertStatistic.getAverage() + "ms on average.\n" +
				"  Inserting a paragraph took at most " + paragraphInsertStatistic.getMax() + "ms.\n" +
				"  Spent " + paragraphInsertStatistic.getSum() + "ms overall on inserting paragraphs.\n" +
				"Inserted " + sentenceInsertStatistic.getCount() + " sentences.\n" +
				"  Inserting a sentence took " + (int) sentenceInsertStatistic.getAverage() + "ms on average.\n" +
				"  Inserting a sentence took at most " + sentenceInsertStatistic.getMax() + "ms.\n" +
				"  Spent " + sentenceInsertStatistic.getSum() + "ms overall on inserting sentences.\n" +
				"Inserted " + tokenInsertStatistic.getCount() + " tokens.\n" +
				"  Inserting a token took " + (int) tokenInsertStatistic.getAverage() + "ms on average.\n" +
				"  Inserting a token took at most " + tokenInsertStatistic.getMax() + "ms.\n" +
				"  Spent " + tokenInsertStatistic.getSum() + "ms overall on inserting tokens.";

		logger.info(statistics);

		// Format statistics as JSON for output files for easier processing
		// later on.
		JSONObject statisticsJSON = new JSONObject();
		statisticsJSON.put(
				"document",
				Formatting.createOutputForMethod(
						"storeJCasDocument", queryHandler
				)
		);
		statisticsJSON.put(
				"paragraph",
				Formatting.createOutputForMethod(
						"storeParagraph", queryHandler
				)
		);
		statisticsJSON.put(
				"sentence",
				Formatting.createOutputForMethod(
						"storeSentence", queryHandler
				)
		);
		statisticsJSON.put(
				"token",
				Formatting.createOutputForMethod(
						"storeToken", queryHandler
				)
		);
		statisticsJSON.put(
				"specificDocumentStatistics",
				this.specificDocumentStatistics
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
