package uimadatabase.dbtest.evaluationFramework;

import dbtest.evaluationFramework.BaseOutputProvider;
import dbtest.evaluationFramework.OutputProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseOutputProviderTest
{
	@BeforeAll
	static void beforeall()
	{
		if (Files.exists(Paths.get("src/test/resources/outputDirectory")))
		{
			try
			{
				Files.delete(Paths.get("src/test/resources/outputDirectory"));
				Files.createDirectory(Paths.get("src/test/resources/outputDirectory"));
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	@Test
	void Given_BaseOutputProviderInitializedWithTestDirectory_When_AskingForNewFile_WillCreateFileInSaidDirectory()
	{
		String path = "src/test/resources/outputDirectory";
		OutputProvider provider = new BaseOutputProvider(path);
		File testFile = provider.createFile(this.getClass().getName(), "testFile");
		Path expectedPath = FileSystems.getDefault().getPath(
				"src/test/resources/outputDirectory",
				this.getClass().getName() + "_testFile.txt"
		);
		assertTrue(Files.exists(expectedPath));
		assertEquals(
				expectedPath.toAbsolutePath(),
				testFile.getAbsolutePath()
		);
	}
}
