package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.IStringConverter;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.implementation.*;

public class EvaluationNameConverter implements IStringConverter<EvaluationCase>
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
		// No error checking necessary, since validators are run beforehand.
		return Evaluations.create(s);
	}
}
