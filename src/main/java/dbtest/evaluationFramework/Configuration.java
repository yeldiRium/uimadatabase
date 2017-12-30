package dbtest.evaluationFramework;

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

	protected OutputProvider outputProvider;

	public List<EvaluationCase> getEvaluations()
	{
		return evaluations;
	}

	public void setEvaluations(List<EvaluationCase> evaluations)
	{
		this.evaluations = evaluations;
	}

	public OutputProvider getOutputProvider()
	{
		return outputProvider;
	}

	public void setOutputProvider(OutputProvider outputProvider)
	{
		this.outputProvider = outputProvider;
	}
}
