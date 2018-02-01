package org.hucompute.service.uima.eval.evaluationFramework.testEvaluations;

import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.hucompute.services.uima.eval.evaluation.framework.exceptions.EvaluationFailedRerunnableException;

public class TestEvaluationFailingRerun implements EvaluationCase
{
	public static int runCounter = 0;

	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(TestConnection.class);
		TestEvaluationB.connectionRequest = connectionRequest;
		return connectionRequest;
	}

	@Override
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	) throws EvaluationFailedRerunnableException
	{
		TestEvaluationFailingRerun.runCounter++;
		if (TestEvaluationFailingRerun.runCounter < 3)
		{
			// Fails two times
			throw new EvaluationFailedRerunnableException();
		}
	}
}
