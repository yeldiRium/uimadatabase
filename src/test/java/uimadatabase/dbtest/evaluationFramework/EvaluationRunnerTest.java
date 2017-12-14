package uimadatabase.dbtest.evaluationFramework;

import dbtest.evaluationFramework.EvaluationRunner;
import org.junit.jupiter.api.Test;
import uimadatabase.dbtest.evaluationFramework.testEvaluations.TestEvaluationA;
import uimadatabase.dbtest.evaluationFramework.testEvaluations.TestEvaluationB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class EvaluationRunnerTest
{
	@Test
	void Given_SimpleConfigFile_When_InstantiatingEvaluationRunner_Then_EvaluationCaseObjectsAreCreatedCorrectly()
	{
		try
		{
			InputStream configFile = new FileInputStream("src/test/resources/evaluationFramework/testConfig.yml");
			EvaluationRunner evaluationRunner = new EvaluationRunner(configFile);
			assertTrue(TestEvaluationA.wasInstantiated);
			assertTrue(TestEvaluationB.wasInstantiated);
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
