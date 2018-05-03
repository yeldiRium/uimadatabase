package org.hucompute.services.uima.eval.evaluation.framework;

import org.hucompute.services.uima.eval.database.abstraction.QueryHandlerInterface;
import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.evaluation.framework.exceptions.EvaluationFailedRerunnableException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class EvaluationRunner implements Runnable
{
	protected static Logger logger = Logger.getLogger(EvaluationRunner.class.getName());

	protected OutputProvider outputProvider;
	protected List<EvaluationCase> evaluations;
	protected List<Class<? extends Connection>> connections;
	protected ConnectionManager connectionManager;

	public EvaluationRunner(
			List<EvaluationCase> evaluations,
			List<Class<? extends Connection>> connections,
			ConnectionManager connectionManager,
			OutputProvider outputProvider
	)
	{
		this.evaluations = evaluations;
		this.connections = connections;
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
		ConnectionRequest connectionRequest = new ConnectionRequest(
				this.connections
		);
		ConnectionResponse connectionResponse = null;
		try
		{
			connectionResponse = this.connectionManager
					.submitRequest(connectionRequest).get();
		} catch (InterruptedException | ExecutionException e)
		{
			e.printStackTrace();
			Thread.currentThread().interrupt();
			return;
		}

		Collection<QueryHandlerInterface> queryHandlers = new ArrayList<>();
		for (Connection connection : connectionResponse.getConnections())
		{
			queryHandlers.add(
					QueryHandlerInterface
							.createQueryHandlerForConnection(connection)
			);
		}

		for (EvaluationCase evaluationCase
				: this.evaluations)
		{
			logger.info("Running EvaluationCase "
					+ evaluationCase.getClass().getName());
			boolean success = false;
			while (!success)
			{
				try
				{
					evaluationCase.run(
							queryHandlers,
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
