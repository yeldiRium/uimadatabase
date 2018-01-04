package dbtest.evaluations.collectionWriter;

import dbtest.connection.implementation.Neo4jConnection;
import dbtest.queryHandler.implementation.Neo4jQueryHandler;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.neo4j.driver.v1.Driver;

public class Neo4jCollectionWriter extends EvaluatingCollectionWriter
{
	private Neo4jQueryHandler queryHandler;

	/**
	 * Must be called after initialization, but before use in a pipeline.
	 * @param connection A Neo4jConnection that has been established.
	 */
	public void injectConnection(Neo4jConnection connection)
	{
		Driver driver = connection.getDriver();
		this.queryHandler = new Neo4jQueryHandler(driver);
	}

	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException
	{
		this.queryHandler.storeJCasDocument(jCas);
	}
}
