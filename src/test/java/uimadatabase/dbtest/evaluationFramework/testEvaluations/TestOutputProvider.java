package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.evaluationFramework.OutputProvider;

import java.io.File;

public class TestOutputProvider implements OutputProvider
{
	@Override
	public File createFile(String caller, String name)
	{
		return null;
	}

	@Override
	public File createFile(String caller, String name, boolean keepOld)
	{
		return null;
	}
}
