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
					new FileInputStream("src/main/resources/config.yml"),
					connectionManager
			);
			System.out.println("Running Evaluations...");
			evaluationRunner.run();
			System.out.println("Evaluations done. Closing connections...");
			connectionManager.close();
			System.out.println("Connections closed. Exiting...");

		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
