package org.hucompute.service.uima.eval.evaluationFramework.testEvaluations;

import org.hucompute.services.uima.eval.database.connection.Connection;

public class TestConnection extends Connection
{
	@Override
	protected boolean tryToConnect()
	{
		return true;
	}

	@Override
	public void close()
	{

	}
}
