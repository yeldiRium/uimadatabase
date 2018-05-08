package org.hucompute.service.uima.eval.evaluationFramework;

import org.hucompute.services.uima.eval.evaluation.framework.BaseOutputProvider;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

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
			throws IOException
	{
		String path = "src/test/resources/outputDirectory";
		OutputProvider provider = new BaseOutputProvider(path);
		provider.configurePath(path);
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
	}

	@Test
	void Given_BaseOutputProviderInitializedWithTestDirectory_When_AskingForNewFileThatExistsAndTellingToBackup_Will_CreateNewFileAndBackupOldOne()
			throws IOException
	{
		String path = "src/test/resources/outputDirectory";
		OutputProvider provider = new BaseOutputProvider(path);
		provider.configurePath(path);
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
	}

	@Test
	void Given_BaseOutputProviderInitializedWithTestDirectory_When_AskingForNewFileThatExistsAndTellingNotToBackup_Will_CreateNewFileAndNotBackupOldOne()
			throws IOException
	{
		String path = "src/test/resources/outputDirectory";
		OutputProvider provider = new BaseOutputProvider(path);
		provider.configurePath(path);
		File testFileFirst = provider.createFile(this.getClass().getName(), "testFile");
		File testFileSecond = provider.createFile(this.getClass().getName(), "testFile");

		Path expectedPath = FileSystems.getDefault().getPath(
				"src/test/resources/outputDirectory",
				this.getClass().getName() + "_testFile_bak0.txt"
		);
		assertFalse(Files.exists(expectedPath));
	}

	@Test
	void Given_BaseOutputProvider_When_InitializingWithAbsolutePath_Will_WorkAsIntended()
			throws IOException
	{
		String path = FileSystems.getDefault().getPath("src/test/resources/outputDirectory").toAbsolutePath().toString();
		OutputProvider provider = new BaseOutputProvider(path);
		provider.configurePath(path);

		File testFile = provider.createFile(this.getClass().getName(), "testFile");
		Path expectedPath = FileSystems.getDefault().getPath(
				"src/test/resources/outputDirectory",
				this.getClass().getName() + "_testFile.txt"
		);
		assertTrue(Files.exists(expectedPath));
		assertEquals(
				expectedPath.toAbsolutePath().toString(),
				testFile.getAbsolutePath()
		);
	}

	@Test
	void Given_BaseOutputProvider_When_SubmittingJSONObject_Will_CreateFileForObject()
			throws IOException
	{
		String path = "src/test/resources/outputDirectory";
		OutputProvider provider = new BaseOutputProvider(path);
		provider.configurePath(path);

		Path expectedPath = FileSystems.getDefault().getPath(
				"src/test/resources/outputDirectory",
				this.getClass().getName() + "_testJSON.txt"
		);
		provider.writeJSON(
				this.getClass().getName(), "testJSON", new JSONObject()
		);
		assertTrue(Files.exists(expectedPath));
	}

	@Test
	void Given_BaseOutputProvider_When_SubmittingJSONTellingToBackup_Will_CreateNewFileAndBackupOldOne()
			throws IOException
	{
		String path = "src/test/resources/outputDirectory";
		OutputProvider provider = new BaseOutputProvider(path);
		provider.configurePath(path);
		provider.writeJSON(this.getClass().getName(), "testJSON", new JSONObject());
		provider.writeJSON(this.getClass().getName(), "testJSON", new JSONObject(), true);
		provider.writeJSON(this.getClass().getName(), "testJSON", new JSONObject(), true);

		Path expectedPath = FileSystems.getDefault().getPath(
				"src/test/resources/outputDirectory",
				this.getClass().getName() + "_testJSON_bak0.txt"
		);
		assertTrue(Files.exists(expectedPath));

		Path expectedPathSecond = FileSystems.getDefault().getPath(
				"src/test/resources/outputDirectory",
				this.getClass().getName() + "_testJSON_bak1.txt"
		);
		assertTrue(Files.exists(expectedPathSecond));
	}

	@Test
	void Given_BaseOutputProvider_When_SubmittingJSON_Will_WriteToFile()
			throws IOException
	{
		String path = "src/test/resources/outputDirectory";
		OutputProvider provider = new BaseOutputProvider(path);
		provider.configurePath(path);

		Path expectedPath = FileSystems.getDefault().getPath(
				"src/test/resources/outputDirectory",
				this.getClass().getName() + "_testJSON.txt"
		);

		JSONObject testJSON = new JSONObject();
		testJSON.put("dataEntry", new Integer(1337));

		JSONObject subJSON = new JSONObject();
		subJSON.put("blub", "blub");
		testJSON.put("subEntry", subJSON);

		provider.writeJSON(this.getClass().getName(), "testJSON", testJSON);

		String expectedFileContent = testJSON.toString(2);
		String fileContent = Files.newBufferedReader(expectedPath).lines()
				.collect(Collectors.joining("\n"));

		assertEquals(expectedFileContent, fileContent);
	}
}
