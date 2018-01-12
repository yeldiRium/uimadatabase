package dbtest.evaluations.collectionWriter;

import dbtest.connection.*;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.implementation.BenchmarkQueryHandler;
import dbtest.queryHandler.implementation.Neo4jQueryHandler;
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

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.stream.Collector;
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

	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException
	{
		super.initialize(context);

		this.dbName = context.getConfigParameterValue(PARAM_DBNAME).toString();
		logger.info("Initializing CollectionWriter for db " + this.dbName);

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
			this.queryHandler = new BenchmarkQueryHandler(
					connection.getQueryHandler()
			);
		} catch (InterruptedException | ExecutionException e)
		{
			logger.severe("Initialization for CollectionWriter failed. " +
					"Interrupted when requesting connection for "
					+ this.dbName + ".");
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}

		logger.info("Initialized CollectionWriter for db " + this.dbName);
	}

	/**
	 * Iterates over the jCas' structure and inserts all relevant elements into
	 * the database.
	 * @param jCas The document to be processed.
	 * @throws AnalysisEngineProcessException If anything goes wrong at all.
	 */
	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException
	{
		final String documentId = DocumentMetaData.get(jCas)
				.getDocumentId();

		logger.info("Storing jCas '" + documentId + "' into " + this.dbName
				+ "...");
		long start = System.currentTimeMillis();
		this.queryHandler.storeJCasDocument(jCas);

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
				previousSentence = sentence;


				/*
				 * Store each element of the jCas that was annotated as a Token
				 * and is contained in the current sentence.
				 */
				Token previousToken = null;
				for (Token token : JCasUtil.selectCovered(
						jCas, Token.class, sentence
				))
				{
					this.queryHandler.storeToken(
							token, jCas, paragraph, sentence, previousToken
					);
					previousToken = token;
				}
			}
		}
		long end = System.currentTimeMillis();

		logger.info("JCas processed and stored.");
		logger.info("Took " + (end - start) + "ms.");

		int storedParagraphs = this.queryHandler.getMethodBenchmarks()
				.get("storeParagraph").getCallCount();
		int averageParagraphInsertTime = (int)(this.queryHandler
				.getMethodBenchmarks().get("storeParagraph").getCallTimes()
				.stream().mapToLong(Long::longValue).sum()
				/ (double) storedParagraphs);
		logger.info(storedParagraphs + " paragraphs were inserted with an " +
				"average insert-time of " + averageParagraphInsertTime + "ms.");

		int storedSentences = this.queryHandler.getMethodBenchmarks()
				.get("storeSentence").getCallCount();
		int averageSentenceInsertTime = (int)(this.queryHandler
				.getMethodBenchmarks().get("storeSentence").getCallTimes()
				.stream().mapToLong(Long::longValue).sum()
				/ (double) storedParagraphs);
		logger.info(storedSentences + " sentences were inserted with an " +
				"average insert-time of " + averageSentenceInsertTime + "ms.");

		int storedTokens = this.queryHandler.getMethodBenchmarks()
				.get("storeToken").getCallCount();
		int averageTokenInsertTime = (int)(this.queryHandler
				.getMethodBenchmarks().get("storeToken").getCallTimes()
				.stream().mapToLong(Long::longValue).sum()
				/ (double) storedParagraphs);
		logger.info(storedTokens + " tokens were inserted " +
				"average insert-time of " + averageTokenInsertTime + "ms.");
	}
}
