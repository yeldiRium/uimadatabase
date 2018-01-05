package dbtest.evaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.implementation.Neo4jQueryHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.StopWatch;

import java.io.IOException;
import java.util.Arrays;

/**
 * Tests several calculation Methods on each Database.
 * Executes full test for each Connection in the ConnectionResponse object.
 * So to configure, which databases should be evaluated, configure the
 * return value for requestConnection accordingly.
 */
public class TTREvaluationCase implements EvaluationCase
{
	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		return connectionRequest;
	}

	@Override
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	)
	{
		// Create QueryHandler from Connections.
		// Then execute Evaluation for each.
		connectionResponse.getConnections().stream()
				.map(QueryHandlerInterface::createQueryHandlerForConnection)
				.forEach(queryHandler -> {
					StringBuilder builder = new StringBuilder();
					long timeBegin;

					builder.append("Testing with QueryHandler '")
							.append(queryHandler.getClass().getName())
							.append("'.\n\n");

					builder.append("Testing calculateTTRForAllDocuments():\n");
					timeBegin = System.currentTimeMillis();
					queryHandler.calculateTTRForAllDocuments();
					builder.append("calculateTTRForAllDocuments() took: ")
							.append(System.currentTimeMillis() - timeBegin)
							.append("  ms\n")

							.append("Testing getTTRForCollectionOfDocuments with two Documents:\n");
					timeBegin = System.currentTimeMillis();
					queryHandler.calculateTTRForCollectionOfDocuments(Arrays.asList("105", "159"));
					builder.append("getTTRForCollectionOfDocuments with two Documnts took: ")
							.append(System.currentTimeMillis() - timeBegin)
							.append("  ms\n")

							.append("Testing countElementsOfType(LEMMA):\n");
					timeBegin = System.currentTimeMillis();
					queryHandler.countElementsOfType(ElementType.Lemma);
					builder.append("countElementsOfType(LEMMA) took: ")
							.append(System.currentTimeMillis() - timeBegin)
							.append("  ms\n")

							.append("Testing countElementsInDocumentOfType(DocId, TOKEN):\n");
					timeBegin = System.currentTimeMillis();
					queryHandler.countElementsInDocumentOfType("105", ElementType.Token);
					builder.append("countElementsInDocumentOfType(DocId, TOKEN) took: ")
							.append(System.currentTimeMillis() - timeBegin)
							.append("  ms\n")

							.append("Testing countElementsOfTypeWithValue(LEMMA, Mensch):\n");
					timeBegin = System.currentTimeMillis();
					queryHandler.countElementsOfTypeWithValue(ElementType.Lemma, "Mensch");
					builder.append("countElementsOfTypeWithValue(LEMMA, Mensch) took: ")
							.append(System.currentTimeMillis() - timeBegin)
							.append(" ms\n");

					try
					{
						FileUtils.writeStringToFile(
								outputProvider.createFile(TTREvaluationCase.class.getName(), "index"),
								builder.toString()
						);
					} catch (IOException e)
					{
						e.printStackTrace();
					}
				});
	}

	// TODO: check if this is needed in the AllQueryEvaluationCase
	@Deprecated
	public static String testQueryTimes(Neo4jQueryHandler queryHandler, int runs) throws Exception
	{
		StopWatch a = new StopWatch();
		a.start();
		a.suspend();
		StopWatch b = new StopWatch();
		b.start();
		b.suspend();
		for (int i = 0; i < runs; i++)
		{
			a.resume();
			queryHandler.getLemmataForDocument("1063");
			a.suspend();

			b.resume();
			queryHandler.getLemmataForDocument("1420");
			b.suspend();
		}
		return a.getTime() + "|" + b.getTime();
	}
}
