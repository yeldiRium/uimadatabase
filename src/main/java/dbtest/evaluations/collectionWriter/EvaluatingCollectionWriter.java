package dbtest.evaluations.collectionWriter;

import dbtest.connection.*;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.implementation.BenchmarkQueryHandler;
import dbtest.queryHandler.implementation.Neo4jQueryHandler;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

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

	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException
	{
		final String documentId = DocumentMetaData.get(jCas)
				.getDocumentId();

		logger.info("Storing jCas '" + documentId + "' into " + this.dbName + "...");
		this.queryHandler.storeJCasDocument(jCas);
		logger.info("JCas processed and stored.");
		List<Long> callTimes = this.queryHandler.getMethodBenchmarks()
				.get("storeJCasDocument").getCallTimes();
		logger.info("Took " + callTimes.get(callTimes.size() - 1) + "ms.");
	}
}
