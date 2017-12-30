package dbtest.evaluationFramework;

import java.io.File;

public interface OutputProvider
{
	public File createFile(String caller, String name);

	public File createFile(String caller, String name, boolean keepOld);
}
