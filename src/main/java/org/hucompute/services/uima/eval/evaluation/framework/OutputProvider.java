package org.hucompute.services.uima.eval.evaluation.framework;

import org.json.JSONObject;

import java.io.File;
import java.io.IOException;

public interface OutputProvider
{
	public void configurePath(String path) throws IOException;

	public File createFile(String caller, String name) throws IOException;

	public File createFile(String caller, String name, boolean keepOld)
			throws IOException;

	public void writeJSON(String caller, String name, JSONObject jsonObject)
			throws IOException;

	public void writeJSON(
			String caller, String name, JSONObject jsonObject, boolean keepOld
	) throws IOException;
}
