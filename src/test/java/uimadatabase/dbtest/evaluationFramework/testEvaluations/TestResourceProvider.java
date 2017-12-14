package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.evaluationFramework.ResourceProvider;

public class TestResourceProvider implements ResourceProvider
{
	public static boolean wasInstantiated = false;

	public TestResourceProvider() {
		TestResourceProvider.wasInstantiated = true;
	}
}
