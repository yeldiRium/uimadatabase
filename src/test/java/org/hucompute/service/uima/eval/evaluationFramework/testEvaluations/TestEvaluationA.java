package org.hucompute.service.uima.eval.evaluationFramework.testEvaluations;

import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;

public class TestEvaluationA implements EvaluationCase
{
	public static boolean wasInstantiated = false;
	public static ConnectionRequest connectionRequest = null;

	public static boolean wasRun = false;
	public static ConnectionResponse connectionResponse = null;

	public TestEvaluationA()
	{
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
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	)
	{
		TestEvaluationA.wasRun = true;
		TestEvaluationA.connectionResponse = connectionResponse;
	}
}
