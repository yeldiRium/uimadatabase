package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.connection.Connection;

public class TestConnection extends Connection
{
	@Override
	protected boolean tryToConnect()
	{
		return true;
	}
}
