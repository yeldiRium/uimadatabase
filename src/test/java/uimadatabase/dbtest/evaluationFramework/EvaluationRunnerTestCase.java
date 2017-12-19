package uimadatabase.dbtest.evaluationFramework;

import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import uimadatabase.dbtest.evaluationFramework.testEvaluations.TestEvaluationA;
import uimadatabase.dbtest.evaluationFramework.testEvaluations.TestEvaluationB;
import uimadatabase.dbtest.evaluationFramework.testEvaluations.TestEvaluationFailingRerun;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EvaluationRunnerTestCase
{
	protected ConnectionManager mockConnectionManager;
	protected Future<ConnectionResponse> futureMockConnectionResponse;
	protected ConnectionResponse mockConnectionResponse;

	@BeforeEach
	void setUpMockConnectionManager()
	{
		ExecutorService executor = Executors.newSingleThreadExecutor();
		this.mockConnectionManager = Mockito.mock(ConnectionManager.class);
		this.mockConnectionResponse = Mockito.mock(ConnectionResponse.class);
		this.futureMockConnectionResponse = executor.submit(() -> mockConnectionResponse);

		when(mockConnectionManager.submitRequest(any())).thenReturn(
				this.futureMockConnectionResponse
		);
	}

	@Test
	void Given_TestConfigFile_When_InstantiatingEvaluationRunner_Then_EvaluationCaseObjectsAreCreatedCorrectly()
	{
		try
		{
			InputStream configFile = new FileInputStream("src/test/resources/evaluationFramework/testConfig.yml");
			EvaluationRunner evaluationRunner = new EvaluationRunner(configFile, this.mockConnectionManager);
			assertTrue(TestEvaluationA.wasInstantiated);
			assertTrue(TestEvaluationB.wasInstantiated);
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	@Test
	void Given_TestConfigFile_When_RunningEvaluationRunner_Then_EvaluationCaseObjectsAreRun()
	{
		try
		{
			InputStream configFile = new FileInputStream("src/test/resources/evaluationFramework/testConfig.yml");
			EvaluationRunner evaluationRunner = new EvaluationRunner(configFile, this.mockConnectionManager);
			evaluationRunner.run();
			assertTrue(TestEvaluationA.wasRun);
			assertTrue(TestEvaluationB.wasRun);
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	@Test
	void Given_TestConfigFile_When_RunningEvaluationRunner_Then_ConnectionManagerReceivesRequests()
	{
		try
		{
			InputStream configFile = new FileInputStream("src/test/resources/evaluationFramework/testConfig.yml");
			EvaluationRunner evaluationRunner = new EvaluationRunner(configFile, this.mockConnectionManager);
			evaluationRunner.run();

			verify(mockConnectionManager).submitRequest(TestEvaluationA.connectionRequest);
			verify(mockConnectionManager).submitRequest(TestEvaluationB.connectionRequest);
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	@Test
	void Given_TestConfigFile_When_RunningEvaluationRunner_Then_EvaluationCasesAreRunWithCorrectConnectionResponse()
	{
		try
		{
			InputStream configFile = new FileInputStream("src/test/resources/evaluationFramework/testConfig.yml");
			EvaluationRunner evaluationRunner = new EvaluationRunner(configFile, this.mockConnectionManager);
			evaluationRunner.run();

			assertSame(this.mockConnectionResponse, TestEvaluationA.connectionResponse);
			assertSame(this.mockConnectionResponse, TestEvaluationB.connectionResponse);
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	@Test
	void Given_TestConfigFileWithFailingEvaluation_When_RunngEvaluationRunnerAndEvaluationFailsWithRerunException_Then_EvaluationIsRerun()
	{
		try
		{
			InputStream configFile = new FileInputStream("src/test/resources/evaluationFramework/testConfigWithFailingRerun.yml");
			EvaluationRunner evaluationRunner = new EvaluationRunner(configFile, this.mockConnectionManager);
			evaluationRunner.run();

			// See TestEvaluationFailingRerun
			// It will fail two times before completing
			assertEquals(3, TestEvaluationFailingRerun.runCounter);
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
