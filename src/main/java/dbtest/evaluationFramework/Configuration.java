package dbtest.evaluationFramework;

import java.util.List;

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
