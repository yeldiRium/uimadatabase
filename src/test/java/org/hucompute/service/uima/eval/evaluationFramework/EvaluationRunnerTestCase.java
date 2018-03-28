package org.hucompute.service.uima.eval.evaluationFramework;

import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationA;
import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationB;
import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationFailingRerun;
import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationRunner;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EvaluationRunnerTestCase
{
	protected ConnectionManager mockConnectionManager;
	protected Future<ConnectionResponse> futureMockConnectionResponse;
	protected ConnectionResponse mockConnectionResponse;
	private OutputProvider mockOutputProvider;

	@BeforeEach
	void setUpMocks()
	{
		ExecutorService executor = Executors.newSingleThreadExecutor();
		this.mockConnectionManager = Mockito.mock(ConnectionManager.class);
		this.mockConnectionResponse = Mockito.mock(ConnectionResponse.class);
		this.futureMockConnectionResponse = executor.submit(() -> mockConnectionResponse);

		when(mockConnectionManager.submitRequest(any())).thenReturn(
				this.futureMockConnectionResponse
		);

		this.mockOutputProvider = Mockito.mock(OutputProvider.class);
	}

	@Test
	void Given_TestConfigFile_When_InstantiatingEvaluationRunner_Then_EvaluationCaseObjectsAreCreatedCorrectly()
	{
		try
		{
			InputStream configFile = new FileInputStream("src/test/resources/evaluationFramework/testConfig.yml");
			EvaluationRunner evaluationRunner = new EvaluationRunner(
					configFile,
					this.mockConnectionManager,
					this.mockOutputProvider
			);
			Assertions.assertTrue(TestEvaluationA.wasInstantiated);
			Assertions.assertTrue(TestEvaluationB.wasInstantiated);
		} catch (IOException e)
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
			EvaluationRunner evaluationRunner = new EvaluationRunner(
					configFile,
					this.mockConnectionManager,
					this.mockOutputProvider
			);
			evaluationRunner.run();
			assertTrue(TestEvaluationA.wasRun);
			assertTrue(TestEvaluationB.wasRun);
		} catch (IOException e)
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
			EvaluationRunner evaluationRunner = new EvaluationRunner(
					configFile,
					this.mockConnectionManager,
					this.mockOutputProvider
			);
			evaluationRunner.run();

			verify(mockConnectionManager).submitRequest(TestEvaluationA.connectionRequest);
			verify(mockConnectionManager).submitRequest(TestEvaluationB.connectionRequest);
		} catch (IOException e)
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
			EvaluationRunner evaluationRunner = new EvaluationRunner(
					configFile,
					this.mockConnectionManager,
					this.mockOutputProvider
			);
			evaluationRunner.run();

			assertSame(this.mockConnectionResponse, TestEvaluationA.connectionResponse);
			assertSame(this.mockConnectionResponse, TestEvaluationB.connectionResponse);
		} catch (IOException e)
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
			EvaluationRunner evaluationRunner = new EvaluationRunner(
					configFile,
					this.mockConnectionManager,
					this.mockOutputProvider
			);
			evaluationRunner.run();

			// See TestEvaluationFailingRerun
			// It will fail two times before completing
			Assertions.assertEquals(3, TestEvaluationFailingRerun.runCounter);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
