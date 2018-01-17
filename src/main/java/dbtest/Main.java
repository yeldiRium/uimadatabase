package dbtest;

import dbtest.connection.ConnectionManager;
import dbtest.evaluationFramework.EvaluationRunner;
import dbtest.logging.PlainFormatter;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Main entrance point for the evaluation system.
 * Creates a ConnectionManager and an EvaluationRunner, which will when run a
 * list of preconfigured EvaluationCases.
 */
public class Main
{
	protected static Logger logger = Logger.getLogger(Main.class.getName());

	public static void main(String[] args)
	{
		// Clean up logging to stdout
		Logger rootLogger = LogManager.getLogManager().getLogger("");
		Formatter plainFormatter = new PlainFormatter();
		for(Handler h : rootLogger.getHandlers())
		{
			h.setFormatter(plainFormatter);
		}

		ConnectionManager connectionManager = ConnectionManager.getInstance();
		try
		{
			EvaluationRunner evaluationRunner = new EvaluationRunner(
					new FileInputStream(System.getenv("CONFIG_PATH")),
					connectionManager
			);
			logger.info("Running Evaluations...");
			evaluationRunner.run();
			logger.info("Evaluations done. Closing connections...");

		} catch (Exception e)
		{
			e.printStackTrace();
			logger.severe("Exception occured. Closing connections and " +
					"stopping threads...");
		} finally
		{
			connectionManager.close();
			logger.info("Connections closed. Exiting...");
		}
	}
}
