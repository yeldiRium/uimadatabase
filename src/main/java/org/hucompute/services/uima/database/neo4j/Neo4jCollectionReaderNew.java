package org.hucompute.services.uima.database.neo4j;

import dbtest.connection.implementation.Neo4jConnection;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.hucompute.services.uima.database.AbstractCollectionReader;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.io.IOException;

public class Neo4jCollectionReaderNew extends AbstractCollectionReader
{
	public static final String PARAM_CONNECTION = "connectionObject";

	protected StatementResult result;

	@Override
	public void initialize(UimaContext context)
			throws ResourceInitializationException
	{
		super.initialize(context);

		Neo4jConnection connection = (Neo4jConnection) context.getConfigParameterValue(Neo4jCollectionReaderNew.PARAM_CONNECTION);
		Driver driver = connection.getDriver();

		try ( Session session = driver.session() )
		{
			this.result = session.readTransaction((tx) -> tx.run("MATCH (n:DOCUMENT) RETURN n"));
		}
	}

	@Override
	public boolean hasNext() throws IOException, CollectionException
	{
		return this.result.hasNext();
	}
	
	@Override
	public void getNext(CAS cas) throws IOException, CollectionException
	{
		Record record = this.result.next();
		// TODO: populate CAS
	}

	@Override
	public Progress[] getProgress()
	{
		// TODO: should maybe be implemented
		return null;
	}
}
