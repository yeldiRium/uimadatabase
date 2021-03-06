package org.hucompute.services.uima.eval.main;

import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.evaluation.framework.BaseOutputProvider;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationRunner;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;

import java.util.List;
import java.util.logging.Logger;

/**
 * EvaluationPipeline entrance point for the evaluation system.
 * Creates a ConnectionManager and an EvaluationRunner, which will when run a
 * list of preconfigured EvaluationCases.
 */
public class EvaluationPipeline
{
	protected static Logger logger = Logger.getLogger(EvaluationPipeline.class.getName());

	public static void run(
			List<EvaluationCase> evaluations,
			List<Class<? extends Connection>> connections
	)
	{
		ConnectionManager connectionManager = ConnectionManager.getInstance();
		try
		{
			OutputProvider outputProvider = new BaseOutputProvider(
					System.getenv("OUTPUT_DIR")
			);
			EvaluationRunner evaluationRunner = new EvaluationRunner(
					evaluations,
					connections,
					connectionManager,
					outputProvider
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
