package uimadatabase.dbtest.evaluationFramework.testEvaluations;

import dbtest.evaluationFramework.OutputProvider;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;

public class TestOutputProvider implements OutputProvider
{
	@Override
	public void configurePath(String path) throws IOException
	{

	}

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

	@Override
	public void writeJSON(String caller, String name, JSONObject jsonObject) throws IOException
	{

	}

	@Override
	public void writeJSON(String caller, String name, JSONObject jsonObject, boolean keepOld) throws IOException
	{

	}
}
