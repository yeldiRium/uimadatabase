package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.exceptions.EvaluationFailedRerunnableException;

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
	public void run(ConnectionResponse connectionResponse) throws EvaluationFailedRerunnableException
	{
		TestEvaluationFailingRerun.runCounter++;
		if (TestEvaluationFailingRerun.runCounter < 3)
		{
			// Fails two times
			throw new EvaluationFailedRerunnableException();
		}
	}
}
