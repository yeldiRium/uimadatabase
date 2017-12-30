package uimadatabase.dbtest.evaluationFramework;

import dbtest.evaluationFramework.BaseOutputProvider;
import dbtest.evaluationFramework.OutputProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseOutputProviderTest
{
	@BeforeEach
	void beforeEach()
	{
		Path outputDirectory = Paths.get("src/test/resources/outputDirectory");
		if (Files.exists(outputDirectory))
		{
			try
			{
				Files.walkFileTree(outputDirectory, new SimpleFileVisitor<Path>()
				{
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
					{
						Files.delete(file);
						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
					{
						Files.delete(dir);
						return FileVisitResult.CONTINUE;
					}
				});
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	@Test
	void Given_BaseOutputProviderInitializedWithTestDirectory_When_AskingForNewFile_WillCreateFileInSaidDirectory()
	{
		try
		{
			String path = "src/test/resources/outputDirectory";
			OutputProvider provider = new BaseOutputProvider(path);
			File testFile = null;
			testFile = provider.createFile(this.getClass().getName(), "testFile");
			Path expectedPath = FileSystems.getDefault().getPath(
					"src/test/resources/outputDirectory",
					this.getClass().getName() + "_testFile.txt"
			);
			assertTrue(Files.exists(expectedPath));
			assertEquals(
					expectedPath.toAbsolutePath().toString(),
					testFile.getAbsolutePath()
			);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	@Test
	void Given_BaseOutputProviderInitializedWithTestDirectory_When_AskingForNewFileThatExistsAndTellingToBackup_WillCreateNewFileAndBackupOldOne()
	{
		try
		{
			String path = "src/test/resources/outputDirectory";
			OutputProvider provider = new BaseOutputProvider(path);
			File testFileFirst = provider.createFile(this.getClass().getName(), "testFile");
			File testFileSecond = provider.createFile(this.getClass().getName(), "testFile", true);
			File testFileThird = provider.createFile(this.getClass().getName(), "testFile", true);

			Path expectedPath = FileSystems.getDefault().getPath(
					"src/test/resources/outputDirectory",
					this.getClass().getName() + "_testFile_bak0.txt"
			);
			assertTrue(Files.exists(expectedPath));

			Path expectedPathSecond = FileSystems.getDefault().getPath(
					"src/test/resources/outputDirectory",
					this.getClass().getName() + "_testFile_bak1.txt"
			);
			assertTrue(Files.exists(expectedPathSecond));
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	@Test
	void Given_BaseOutputProviderInitializedWithTestDirectory_When_AskingForNewFileThatExistsAndTellingNotToBackup_WillCreateNewFileAndNotBackupOldOne()
	{
		try
		{
			String path = "src/test/resources/outputDirectory";
			OutputProvider provider = new BaseOutputProvider(path);
			File testFileFirst = provider.createFile(this.getClass().getName(), "testFile");
			File testFileSecond = provider.createFile(this.getClass().getName(), "testFile");

			Path expectedPath = FileSystems.getDefault().getPath(
					"src/test/resources/outputDirectory",
					this.getClass().getName() + "_testFile_bak0.txt"
			);
			assertFalse(Files.exists(expectedPath));
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
