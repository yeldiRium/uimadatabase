package dbtest.evaluationFramework;

import java.util.List;

public class Configuration
{
	protected List<EvaluationCase> evaluations;
	protected ResourceProvider resourceProvider;

	public List<EvaluationCase> getEvaluations()
	{
		return evaluations;
	}

	public void setEvaluations(List<EvaluationCase> evaluations)
	{
		this.evaluations = evaluations;
	}

	public ResourceProvider getResourceProvider()
	{
		return resourceProvider;
	}

	public void setResourceProvider(ResourceProvider resourceProvider)
	{
		this.resourceProvider = resourceProvider;
	}
}
