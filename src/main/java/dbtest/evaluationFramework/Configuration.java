package dbtest.evaluationFramework;

import java.util.List;

public class Configuration
{
	protected List<EvaluationCase> evaluations;
	protected ResourceProvider resourceProvider;
	protected OutputService outputService;

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

	public OutputService getOutputService()
	{
		return outputService;
	}

	public void setOutputService(OutputService outputService)
	{
		this.outputService = outputService;
	}
}
