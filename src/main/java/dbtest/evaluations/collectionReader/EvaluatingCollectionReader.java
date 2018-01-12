package dbtest.evaluations.collectionReader;

import dbtest.connection.*;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.exceptions.QHException;
import dbtest.queryHandler.implementation.BenchmarkQueryHandler;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class EvaluatingCollectionReader extends CasCollectionReader_ImplBase
{
	protected static final Logger logger =
			Logger.getLogger(EvaluatingCollectionReader.class.getName());

	// UIMA Parameters
	public static final String PARAM_OUTPUT_FILE = "outputFile";
	@ConfigurationParameter(name = PARAM_OUTPUT_FILE, mandatory = false)
	protected File outputFile;

	public static final String PARAM_DBNAME = "dbName";
	@ConfigurationParameter(name = PARAM_DBNAME)
	protected String dbName;

	protected BenchmarkQueryHandler queryHandler;
	protected Iterator<String> iterator;

	@Override
	public void initialize(final UimaContext context)
			throws ResourceInitializationException
	{
		super.initialize(context);

		this.dbName = context.getConfigParameterValue(PARAM_DBNAME).toString();
		logger.info("Initializing CollectionReader for db " + this.dbName);

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
			logger.severe("Initialization for CollectionReader failed. " +
					"Interrupted when requesting connection for "
					+ this.dbName + ".");
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}

		Iterable<String> ids = this.queryHandler.getDocumentIds();
		this.iterator = ids.iterator();

		logger.info("Initialized CollectionReader for db " + this.dbName);
	}

	@Override
	public void getNext(CAS cas) throws IOException, CollectionException
	{
		String id = this.iterator.next();
		try
		{
			logger.info("Populating CAS with document " + id + " from "
					+ this.dbName + "...");
			this.queryHandler.populateCasWithDocument(cas, id);
			logger.info("CAS populated.");
			List<Long> callTimes = this.queryHandler.getMethodBenchmarks()
					.get("populateCasWithDocument").getCallTimes();
			logger.info("Took " + callTimes.get(callTimes.size() - 1)
					+ "ms.");
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
