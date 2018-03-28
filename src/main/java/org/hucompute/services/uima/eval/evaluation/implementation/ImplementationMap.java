package org.hucompute.services.uima.eval.evaluation.implementation;

import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ImplementationMap
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
}
