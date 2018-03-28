package org.hucompute.service.uima.eval.evaluationFramework.testEvaluations;

import org.hucompute.services.uima.eval.database.abstraction.QueryHandlerInterface;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;

import java.util.Collection;
import java.util.List;

public class TestEvaluationB implements EvaluationCase
{
	public static boolean wasInstantiated = false;
	public static boolean wasRun = false;
	public static Collection<QueryHandlerInterface> queryHandlers = null;

	public TestEvaluationB()
	{
		TestEvaluationA.wasInstantiated = true;
	}

	@Override
	public void run(
			Collection<QueryHandlerInterface> queryHandlers,
			OutputProvider outputProvider
	)
	{
		TestEvaluationB.wasRun = true;
		TestEvaluationB.queryHandlers = queryHandlers;
	}
}
