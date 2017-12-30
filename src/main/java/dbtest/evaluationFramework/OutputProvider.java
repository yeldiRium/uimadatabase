package dbtest.evaluationFramework;

import java.io.File;
import java.io.IOException;

public interface OutputProvider
{
	public File createFile(String caller, String name) throws IOException;

	public File createFile(String caller, String name, boolean keepOld) throws IOException;
}
