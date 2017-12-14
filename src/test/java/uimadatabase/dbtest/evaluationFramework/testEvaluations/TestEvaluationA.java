package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.ResourceProvider;

import java.util.logging.Logger;

public class TestEvaluationA implements EvaluationCase
{
	public static boolean wasInstantiated = false;

	public TestEvaluationA() {
		TestEvaluationA.wasInstantiated = true;
	}

	@Override
	public ConnectionRequest requestConnection()
	{
		return null;
	}

	@Override
	public void run(ResourceProvider resourceProvider, Logger logger, ConnectionResponse connectionResponse)
	{

	}
}
