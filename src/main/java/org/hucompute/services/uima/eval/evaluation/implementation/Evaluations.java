package org.hucompute.services.uima.eval.evaluation.implementation;

import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Evaluations
{
	public static final Map<String, EvaluationCase> implementationMap;

	static
	{
		Map<String, EvaluationCase> tempMap = new HashMap<>();
		tempMap.put(
				"read", new AllReadEvaluationCase()
		);
		tempMap.put(
				"write", new AllWriteEvaluationCase()
		);
		tempMap.put(
				"query", new AllQueryEvaluationCase()
		);
		tempMap.put(
				"calculate", new AllCalculateEvaluationCase()
		);
		tempMap.put(
				"complex-query", new AllComplexQueryEvaluationCase()
		);
		implementationMap = Collections.unmodifiableMap(tempMap);
	}

	/**
	 * @param name The name of an EvaluationCase.
	 * @return an instance of the requested EvaluationCase.
	 */
	public static EvaluationCase create(String name)
	{
		if (implementationMap.containsKey(name))
		{
			return implementationMap.get(name);
		}
		throw new IllegalArgumentException();
	}

	/**
	 * @return a set of all valid and creatable EvaluationCases.
	 */
	public static Set<String> names()
	{
		return implementationMap.keySet();
	}
}
