package org.hucompute.service.uima.eval.evaluationFramework;

import com.arangodb.ArangoDB;
import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationA;
import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationB;
import org.hucompute.service.uima.eval.evaluationFramework.testEvaluations.TestEvaluationFailingRerun;
import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.ConnectionManager;
import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.database.connection.implementation.ArangoDBConnection;
import org.hucompute.services.uima.eval.database.connection.implementation.MySQLConnection;
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
	void Given_TestEvaluations_When_RunningEvaluationRunner_Then_EvaluationCaseObjectsAreRun()
	{
		List<EvaluationCase> evaluations = new ArrayList<>();
		evaluations.add(new TestEvaluationA());
		evaluations.add(new TestEvaluationB());
		List<Class<? extends Connection>> connections = new ArrayList<>();
		connections.add(ArangoDBConnection.class);
		connections.add(MySQLConnection.class);
		EvaluationRunner evaluationRunner = new EvaluationRunner(
				evaluations,
				connections,
				this.mockConnectionManager,
				this.mockOutputProvider
		);
		evaluationRunner.run();
		assertTrue(TestEvaluationA.wasRun);
		assertTrue(TestEvaluationB.wasRun);
	}

	@Test
	void Given_TestEvaluationsWithFailingEvaluation_When_RunngEvaluationRunnerAndEvaluationFailsWithRerunException_Then_EvaluationIsRerun()
	{
		List<EvaluationCase> evaluations = new ArrayList<>();
		evaluations.add(new TestEvaluationFailingRerun());

		List<Class<? extends Connection>> connections = new ArrayList<>();
		connections.add(ArangoDBConnection.class);
		connections.add(MySQLConnection.class);

		EvaluationRunner evaluationRunner = new EvaluationRunner(
				evaluations,
				connections,
				this.mockConnectionManager,
				this.mockOutputProvider
		);
		evaluationRunner.run();

		// See TestEvaluationFailingRerun
		// It will fail two times before completing
		Assertions.assertEquals(3, TestEvaluationFailingRerun.runCounter);
	}
}
