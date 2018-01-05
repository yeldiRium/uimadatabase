package dbtest.evaluations.collectionWriter;

import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.queryHandler.implementation.Neo4jQueryHandler;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import java.util.concurrent.ExecutionException;

public class Neo4jCollectionWriter extends EvaluatingCollectionWriter
{
	private Neo4jQueryHandler queryHandler;

	// UIMA Parameters
	public static final String BLUB = "blub";

	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException
	{
		super.initialize(context);
		ConnectionRequest request = new ConnectionRequest();
		request.addRequestedConnection(Neo4jConnection.class);
		try
		{
			ConnectionResponse response = ConnectionManager.getInstance()
					.submitRequest(request).get();
			Neo4jConnection neo4jConnection = (Neo4jConnection) response
					.getConnection(Neo4jConnection.class);
			this.queryHandler = new Neo4jQueryHandler(
					neo4jConnection.getDriver()
			);
		} catch (InterruptedException | ExecutionException e)
		{
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException
	{
		logger.info("Processing jCas...");
		this.queryHandler.storeJCasDocument(jCas);
		logger.info("JCas processed.");
	}
}
