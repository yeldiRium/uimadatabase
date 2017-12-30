package dbtest.evaluationFramework;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class BaseOutputProvider implements OutputProvider
{
	Path outputDirectory;

	public BaseOutputProvider(String outputDirectory)
	{
		this.outputDirectory = FileSystems.getDefault().getPath(outputDirectory);
		if (!Files.exists(this.outputDirectory))
		{
			try
			{
				Files.createDirectory(this.outputDirectory);
			} catch (IOException e)
			{
				e.printStackTrace(); // TODO: improve exception handling
			}
		}
	}

	@Override
	public File createFile(String caller, String name)
	{
		return this.createFile(caller, name, false);
	}

	@Override
	public File createFile(String caller, String name, boolean keepOld)
	{
		return null;
	}
}
