package dbtest;

import dbtest.connection.ConnectionManager;
import dbtest.evaluationFramework.EvaluationRunner;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Main entrance point for the evaluation system.
 * Creates a ConnectionManager and an EvaluationRunner, which will when run a
 * list of preconfigured EvaluationCases.
 */
public class Main
{
	public static void main(String[] args)
	{
		try
		{
			ConnectionManager connectionManager = ConnectionManager.getInstance();
			EvaluationRunner evaluationRunner = new EvaluationRunner(
					new FileInputStream("src/main/resources/config.yml"),
					connectionManager
			);
			System.out.println("Running Evaluations...");
			evaluationRunner.run();
			System.out.println("Evaluations done. Closing connections...");
			connectionManager.close();
			System.out.println("Connections closed. Exiting...");

		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
