package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.ResourceProvider;

import java.util.logging.Logger;

public class TestEvaluationB implements EvaluationCase
{
	public static boolean wasInstantiated = false;
	public static boolean wasRun = false;
	public static ConnectionRequest connectionRequest = null;

	public TestEvaluationB() {
		TestEvaluationB.wasInstantiated = true;
	}

	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(TestConnection.class);
		TestEvaluationB.connectionRequest = connectionRequest;
		return connectionRequest;
	}

	@Override
	public void run(ResourceProvider resourceProvider, Logger logger, ConnectionResponse connectionResponse)
	{
		TestEvaluationB.wasRun = true;
	}
}
