package org.hucompute.services.uima.eval.main;

import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationRunner;
import org.hucompute.services.uima.eval.utility.logging.PlainFormatter;

import java.io.FileInputStream;
import java.util.logging.*;

/**
 * EvaluationPipeline entrance point for the evaluation system.
 * Creates a ConnectionManager and an EvaluationRunner, which will when run a
 * list of preconfigured EvaluationCases.
 */
public class EvaluationPipeline
{
	protected static Logger logger = Logger.getLogger(EvaluationPipeline.class.getName());

	public static void main(String[] args)
	{
		// Clean up logging to stdout
		Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(Level.ALL);
		Formatter plainFormatter = new PlainFormatter();
		for(Handler h : rootLogger.getHandlers())
		{
			h.setFormatter(plainFormatter);
			h.setLevel(Level.ALL);
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
		System.exit(0);
	}
}
