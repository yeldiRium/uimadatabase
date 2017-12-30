package dbtest.evaluationFramework;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class BaseOutputProvider implements OutputProvider
{
	Path outputDirectory;

	public BaseOutputProvider(String outputDirectory) throws IOException
	{
		this.outputDirectory = FileSystems.getDefault().getPath(outputDirectory);
		if (!Files.exists(this.outputDirectory))
		{
			Files.createDirectory(this.outputDirectory);
		}
	}

	@Override
	public File createFile(String caller, String name) throws IOException
	{
		return this.createFile(caller, name, false);
	}

	/**
	 * Creates an output file to write to.
	 * If an output file for the given caller and name already exists, the operation
	 * depends on keepOld:
	 * If the old file should be kept, it is backed up (respecting already existing backups).
	 * If it should not be kept, it is deleted.
	 * Then a new file is created and returned.
	 * @param caller
	 * @param name
	 * @param keepOld
	 * @return
	 * @throws IOException
	 */
	@Override
	public File createFile(String caller, String name, boolean keepOld) throws IOException
	{
		String fileName = caller + "_" + name;
		String extension = ".txt";
		Path filePath = this.outputDirectory.resolve(fileName + extension);
		if (Files.exists(filePath))
		{
			if (keepOld)
			{
				boolean moved = false;
				int backup_counter = 0;
				while (!moved)
				{
					try
					{
						Files.move(filePath, filePath.resolveSibling(fileName + "_bak" + String.valueOf(backup_counter) + extension));
						moved = true;
					} catch (IOException e)
					{
						backup_counter++;
					}
				}
			} else
			{
				Files.delete(filePath);
			}
		}
		File newFile = filePath.toFile();
		newFile.createNewFile(); // Should always be true, since we delete/backup the file before. Concurrent access could be a problem.
		return newFile;
	}
}
