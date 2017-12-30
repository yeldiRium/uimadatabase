package dbtest.evaluationFramework;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Manages an output directory and creates and backs up files for others to use.
 */
public class BaseOutputProvider implements OutputProvider
{
	Path outputDirectory;

	/**
	 * Creates the outputDirectory if necessary.
	 * @param outputDirectory The directory where output files will be created in.
	 * @throws IOException if the directory doesn't exist and can't be created.
	 */
	public BaseOutputProvider(String outputDirectory) throws IOException
	{
		this.outputDirectory = FileSystems.getDefault().getPath(outputDirectory);
		if (!Files.exists(this.outputDirectory))
		{
			Files.createDirectory(this.outputDirectory);
		}
	}

	/**
	 * Overload for #createFile with keepOld=false as default.
	 * @param caller
	 * @param name
	 * @return
	 * @throws IOException
	 */
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
	 * @param caller Should be the class calling this method.
	 * @param name The name for the output file.
	 * @param keepOld If a possibly existing output file with the same name should be kept or removed.
	 * @return The output file for further use.
	 * @throws IOException If a file with the same name exists and can't be deleted.
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
