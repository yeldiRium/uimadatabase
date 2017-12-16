package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputService;
import dbtest.evaluationFramework.ResourceProvider;

import java.util.logging.Logger;

public class TestEvaluationB implements EvaluationCase
{
	public static boolean wasInstantiated = false;
	public static ConnectionRequest connectionRequest = null;

	public static boolean wasRun = false;
	public static ResourceProvider resourceProvider = null;
	public static OutputService outputService = null;
	public static ConnectionResponse connectionResponse = null;

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
	public void run(ResourceProvider resourceProvider, OutputService outputService, ConnectionResponse connectionResponse)
	{
		TestEvaluationB.wasRun = true;
		TestEvaluationB.resourceProvider = resourceProvider;
		TestEvaluationB.outputService = outputService;
		TestEvaluationB.connectionResponse = connectionResponse;
	}
}
