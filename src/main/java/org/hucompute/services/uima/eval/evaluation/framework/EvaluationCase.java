package org.hucompute.services.uima.eval.evaluation.framework;

import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.evaluation.framework.exceptions.EvaluationFailedRerunnableException;
import org.hucompute.services.uima.eval.evaluation.framework.exceptions.EvaluationFailedRerunnableException;

import java.io.IOException;

public interface EvaluationCase
{
	ConnectionRequest requestConnection();

	/**
	 * @param connectionResponse Contains all Connections requested in
	 *                           #requestConnection().
	 * @throws EvaluationFailedRerunnableException when a failing action can be
	 *                                             fixed by rerunning the
	 *                                             EvaluationCase.
	 */
	void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	) throws EvaluationFailedRerunnableException, IOException;
}
