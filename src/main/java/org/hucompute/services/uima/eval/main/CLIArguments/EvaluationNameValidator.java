package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import org.hucompute.services.uima.eval.evaluation.implementation.ImplementationMap;

public class EvaluationNameValidator implements IParameterValidator
{
	@Override
	public void validate(String name, String evaluations) throws ParameterException
	{
		String[] evaluationNames = evaluations.split(",");
		for (int i = 0; i < evaluationNames.length; i++)
		{
			String evaluationName = evaluationNames[i];
			if (!ImplementationMap.implementationMap.containsKey(evaluationName))
			{
				this.wrongEvaluationName(evaluationName);
			}
			for (int j = 0; i < evaluationNames.length; i++)
			{
				if (i == j)
				{
					continue;
				}
				if (evaluationNames[i].equals(evaluationNames[j]))
				{
					throw new ParameterException("The evaluation name \"" +
							evaluationName + "\" is duplicated. Please specify " +
							"each evaluation only once.");
				}
			}
		}
	}

	private void wrongEvaluationName(String evaluationName)
	{
		String availableDatabaseNames = String.join(
				", ",
				ImplementationMap.implementationMap.keySet()
		);
		throw new ParameterException("The evaluation name \"" + evaluationName +
				"\" is not valid. Available values are: " +
				availableDatabaseNames);
	}
}
