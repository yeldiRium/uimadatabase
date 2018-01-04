package dbtest.evaluations.collectionReader;

import dbtest.connection.implementation.Neo4jConnection;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.exceptions.QHException;
import dbtest.queryHandler.implementation.Neo4jQueryHandler;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.neo4j.driver.v1.Driver;

import java.io.IOException;
import java.util.Iterator;

public class Neo4jCollectionReader extends EvaluatingCollectionReader
{
	public static final String PARAM_CONNECTION = "connectionObject";

	protected QueryHandlerInterface queryHandler;
	protected Iterator<String> iterator;

	@Override
	public void initialize(final UimaContext context)
			throws ResourceInitializationException
	{
		super.initialize(context);

		System.out.println("Neo4jCollectionReaderInitialized.");
	}

	/**
	 * Must be called after initialization, but before use in a pipeline.
	 * @param connection A Neo4jConnection that has been established.
	 */
	public void injectConnection(Neo4jConnection connection)
	{
		Driver driver = connection.getDriver();
		this.queryHandler = new Neo4jQueryHandler(driver);
		Iterable<String> ids = this.queryHandler.getDocumentIds();
		this.iterator = ids.iterator();
	}

	@Override
	public void getNext(CAS cas) throws IOException, CollectionException
	{
		String id = this.iterator.next();
		try
		{
			logger.info("Populating CAS with document " + id + "...");
			this.queryHandler.populateCasWithDocument(cas, id);
			logger.info("CAS populated.");
		} catch (QHException e)
		{
			if (e.getException() instanceof CASException)
			{
				throw new CollectionException(e.getException());
			}
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() throws IOException, CollectionException
	{
		return this.iterator.hasNext();
	}

	@Override
	public Progress[] getProgress()
	{
		return new Progress[0];
	}
}
