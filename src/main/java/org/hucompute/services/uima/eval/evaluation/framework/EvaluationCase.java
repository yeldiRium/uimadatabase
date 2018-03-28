package org.hucompute.services.uima.eval.evaluation.framework;

import org.hucompute.services.uima.eval.database.abstraction.QueryHandlerInterface;
import org.hucompute.services.uima.eval.evaluation.framework.exceptions.EvaluationFailedRerunnableException;

import java.io.IOException;
import java.util.Collection;

public interface EvaluationCase
{
	/**
	 * @param queryHandlers All QueryHandlers for which the evaluation should be
	 *                    run.
	 * @throws EvaluationFailedRerunnableException when a failing action can be
	 *                                             fixed by rerunning the
	 *                                             EvaluationCase.
	 */
	void run(
			Collection<QueryHandlerInterface> queryHandlers,
			OutputProvider outputProvider
	) throws EvaluationFailedRerunnableException, IOException;
}
