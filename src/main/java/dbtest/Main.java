package dbtest;

import dbtest.connection.ConnectionManager;
import dbtest.evaluationFramework.EvaluationRunner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class Main
{
	public static void main(String[] args)
	{
		try
		{
			ConnectionManager connectionManager = new ConnectionManager();
			EvaluationRunner evaluationRunner = new EvaluationRunner(
				new FileInputStream("src/main/resoures/config.yml"),
				connectionManager
			);
			evaluationRunner.run();
			connectionManager.close();
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
