package org.hucompute.service.uima.eval.evaluationFramework.testEvaluations;

import org.hucompute.services.uima.eval.database.abstraction.QueryHandlerInterface;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.hucompute.services.uima.eval.evaluation.framework.exceptions.EvaluationFailedRerunnableException;

import java.util.Collection;

public class TestEvaluationFailingRerun implements EvaluationCase
{
	public static int runCounter = 0;

	@Override
	public void run(
			Collection<QueryHandlerInterface> queryHandlers,
			OutputProvider outputProvider
	) throws EvaluationFailedRerunnableException
	{
		TestEvaluationFailingRerun.runCounter++;
		if (TestEvaluationFailingRerun.runCounter < 3)
		{
			// Fails two times
			throw new EvaluationFailedRerunnableException();
		}
	}
}
