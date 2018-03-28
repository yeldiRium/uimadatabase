package org.hucompute.services.uima.eval.evaluation.framework;

import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.evaluation.framework.exceptions.EvaluationFailedRerunnableException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class EvaluationRunner implements Runnable
{
	protected static Logger logger = Logger.getLogger(EvaluationRunner.class.getName());

	protected OutputProvider outputProvider;
	protected List<EvaluationCase> evaluations;
	protected ConnectionManager connectionManager;

	public EvaluationRunner(
			List<EvaluationCase> evaluations,
			ConnectionManager connectionManager,
			OutputProvider outputProvider
	)
	{
		this.evaluations = evaluations;
		this.connectionManager = connectionManager;
		this.outputProvider = outputProvider;
	}

	/**
	 * Executes all configured EvaluationCases in order.
	 * Passes each its according connectionResponse once it is ready and an
	 * OutputProvider instance for logging.
	 */
	@Override
	public void run()
	{
		for (EvaluationCase evaluationCase
				: this.evaluations)
		{
			ConnectionRequest connectionRequest =
					evaluationCase.requestConnection();
			ConnectionResponse connectionResponse = null;
			try
			{
				connectionResponse = this.connectionManager
						.submitRequest(connectionRequest).get();
			} catch (InterruptedException | ExecutionException e)
			{
				Thread.currentThread().interrupt();
			}
			logger.info("Running EvaluationCase "
					+ evaluationCase.getClass().getName());
			boolean success = false;
			while (!success)
			{
				try
				{
					evaluationCase.run(
							connectionResponse,
							this.outputProvider
					);
					success = true;
				} catch (EvaluationFailedRerunnableException e)
				{
					logger.info("Evaluation failed. Rerunning...");
				} catch (IOException e)
				{
					// TODO: handle better
					e.printStackTrace();
				}
			}
		}
	}
}
