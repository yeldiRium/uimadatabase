package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.ResourceProvider;

import java.util.logging.Logger;

public class TestEvaluationA implements EvaluationCase
{
	public static boolean wasInstantiated = false;
	public static boolean wasRun = false;
	public static ConnectionRequest connectionRequest = null;

	public TestEvaluationA() {
		TestEvaluationA.wasInstantiated = true;
	}

	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(TestConnection.class);
		TestEvaluationA.connectionRequest = connectionRequest;
		return connectionRequest;
	}

	@Override
	public void run(ResourceProvider resourceProvider, Logger logger, ConnectionResponse connectionResponse)
	{
		TestEvaluationA.wasRun = true;
	}
}
