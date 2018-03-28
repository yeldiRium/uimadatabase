package org.hucompute.service.uima.eval.evaluationFramework;

import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationA;
import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationB;
import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationFailingRerun;
import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationRunner;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
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
	void Given_TestConfigFile_When_RunningEvaluationRunner_Then_EvaluationCaseObjectsAreRun()
	{
		List<EvaluationCase> evaluations = new ArrayList<>();
		evaluations.add(new TestEvaluationA());
		evaluations.add(new TestEvaluationB());
		EvaluationRunner evaluationRunner = new EvaluationRunner(
				evaluations,
				this.mockConnectionManager,
				this.mockOutputProvider
		);
		evaluationRunner.run();
		assertTrue(TestEvaluationA.wasRun);
		assertTrue(TestEvaluationB.wasRun);
	}

	@Test
	void Given_TestConfigFile_When_RunningEvaluationRunner_Then_ConnectionManagerReceivesRequests()
	{
		List<EvaluationCase> evaluations = new ArrayList<>();
		evaluations.add(new TestEvaluationA());
		evaluations.add(new TestEvaluationB());
		EvaluationRunner evaluationRunner = new EvaluationRunner(
				evaluations,
				this.mockConnectionManager,
				this.mockOutputProvider
		);
		evaluationRunner.run();

		verify(mockConnectionManager).submitRequest(TestEvaluationA.connectionRequest);
		verify(mockConnectionManager).submitRequest(TestEvaluationB.connectionRequest);
	}

	@Test
	void Given_TestConfigFile_When_RunningEvaluationRunner_Then_EvaluationCasesAreRunWithCorrectConnectionResponse()
	{
		List<EvaluationCase> evaluations = new ArrayList<>();
		evaluations.add(new TestEvaluationA());
		evaluations.add(new TestEvaluationB());
		EvaluationRunner evaluationRunner = new EvaluationRunner(
				evaluations,
				this.mockConnectionManager,
				this.mockOutputProvider
		);
		evaluationRunner.run();

		assertSame(this.mockConnectionResponse, TestEvaluationA.connectionResponse);
		assertSame(this.mockConnectionResponse, TestEvaluationB.connectionResponse);
	}

	@Test
	void Given_TestConfigFileWithFailingEvaluation_When_RunngEvaluationRunnerAndEvaluationFailsWithRerunException_Then_EvaluationIsRerun()
	{
		List<EvaluationCase> evaluations = new ArrayList<>();
		evaluations.add(new TestEvaluationFailingRerun());
		EvaluationRunner evaluationRunner = new EvaluationRunner(
				evaluations,
				this.mockConnectionManager,
				this.mockOutputProvider
		);
		evaluationRunner.run();

		// See TestEvaluationFailingRerun
		// It will fail two times before completing
		Assertions.assertEquals(3, TestEvaluationFailingRerun.runCounter);
	}
}
