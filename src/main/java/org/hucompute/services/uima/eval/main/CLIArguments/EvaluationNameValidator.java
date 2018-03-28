package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import org.hucompute.services.uima.eval.evaluation.implementation.Evaluations;

public class EvaluationNameValidator implements IParameterValidator
{
	@Override
	public void validate(String name, String evaluations)
			throws ParameterException
	{
		String[] evaluationNames = evaluations.split(",");
		for (int i = 0; i < evaluationNames.length; i++)
		{
			String evaluationName = evaluationNames[i];
			if (!Evaluations.names().contains(evaluationName))
			{
				this.wrongEvaluationName(evaluationName);
			}

			for (int j = 0; j < evaluationNames.length; j++)
			{
				if (i != j && evaluationNames[i].equals(evaluationNames[j]))
				{
					throw new ParameterException("The evaluation name \"" +
							evaluationName + "\" is duplicated. Please specify " +
							"each evaluation only once.");
				}
			}
		}
	}

	private void wrongEvaluationName(String evaluationName)
			throws ParameterException
	{
		String availableDatabaseNames = String.join(
				", ",
				Evaluations.names()
		);
		throw new ParameterException("The evaluation name \"" + evaluationName +
				"\" is not valid. Available values are: " +
				availableDatabaseNames);
	}
}
