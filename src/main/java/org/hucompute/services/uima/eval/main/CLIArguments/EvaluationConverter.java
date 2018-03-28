package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.IStringConverter;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.implementation.*;

public class EvaluationConverter implements IStringConverter<EvaluationCase>
{
	/**
	 * Very basic switch to identify Evaluations.
	 * Has to be expanded if new evaluations are added.
	 *
	 * @param s A user-entered name for an evaluation.
	 * @return The according EvaluationCase
	 */
	@Override
	public EvaluationCase convert(String s)
	{
		switch (s)
		{
			case "read":
				return new AllReadEvaluationCase();
			case "write":
				return new AllWriteEvaluationCase();
			case "query":
				return new AllQueryEvaluationCase();
			case "calculate":
				return new AllCalculateEvaluationCase();
			case "complex-query":
				return new AllComplexQueryEvaluationCase();
			default:
				return null;
		}
	}
}
