package dbtest.evaluations.collectionReaders;

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
import org.hucompute.services.uima.database.neo4j.Neo4jCollectionReaderNew;
import org.neo4j.driver.v1.Driver;

import java.io.IOException;
import java.util.Iterator;

public class Neo4jCollectionReader extends EvaluatingCollectionReader
{
	protected QueryHandlerInterface queryHandler;
	protected Iterator<String> iterator;

	@Override
	public void initialize(final UimaContext context)
			throws ResourceInitializationException
	{
		super.initialize(context);

		Neo4jConnection connection = (Neo4jConnection) context.getConfigParameterValue(Neo4jCollectionReaderNew.PARAM_CONNECTION);
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
			this.queryHandler.populateCasWithDocument(cas, id);
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
