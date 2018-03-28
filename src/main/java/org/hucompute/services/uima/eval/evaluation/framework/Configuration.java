package org.hucompute.services.uima.eval.evaluation.framework;

import java.util.List;

/**
 * Configuration object for the EvaluationRunner.
 * Determines, what information is loaded from the yaml config file.
 * See snakeyaml library for details.
 * https://bitbucket.org/asomov/snakeyaml/wiki/Documentation
 */
public class Configuration
{
	protected List<EvaluationCase> evaluations;

	public List<EvaluationCase> getEvaluations()
	{
		return evaluations;
	}

	public void setEvaluations(List<EvaluationCase> evaluations)
	{
		this.evaluations = evaluations;
	}
}
