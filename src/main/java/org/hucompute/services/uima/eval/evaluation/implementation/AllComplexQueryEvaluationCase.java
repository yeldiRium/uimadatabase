package org.hucompute.services.uima.eval.evaluation.implementation;

import com.google.common.collect.Sets;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.implementation.BenchmarkQueryHandler;
import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.ConnectionRequest;
import org.hucompute.services.uima.eval.database.connection.ConnectionResponse;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.hucompute.services.uima.eval.database.connection.implementation.MySQLConnection;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.hucompute.services.uima.eval.utility.Collections;
import org.hucompute.services.uima.eval.utility.Formatting;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Evaluates the times for all complex query methods on QueryHandlers. This
 * currently means all methods that retrieve two- or tri-grams from the
 * database.
 * <p>
 * Expects data to exists in the database. Accomplish this by running it after
 * the AllWriteEvaluationCase.
 */
public class AllComplexQueryEvaluationCase implements EvaluationCase
{
	protected static final Logger logger =
			Logger.getLogger(AllComplexQueryEvaluationCase.class.getName());

	protected Connections.DBName dbName;
	protected Set<String> documentIds;

	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
//		connectionRequest.addRequestedConnection(ArangoDBConnection.class);
//		connectionRequest.addRequestedConnection(BaseXConnection.class);
//		connectionRequest.addRequestedConnection(CassandraConnection.class);
//		connectionRequest.addRequestedConnection(MongoDBConnection.class);
		connectionRequest.addRequestedConnection(MySQLConnection.class);
//		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		return connectionRequest;
	}

	@Override
	public void run(
			ConnectionResponse connectionResponse, OutputProvider outputProvider
	) throws IOException
	{
		int inputFiles = new File(System.getenv("INPUT_DIR")).list().length;
		for (Connection connection : connectionResponse.getConnections())
		{
			this.dbName =
					Connections.getIdentifierForConnectionClass(
							connection.getClass()
					);

			logger.info("Starting AllComplexQueryEvaluationCase for Database \""
					+ this.dbName + "\".");

			BenchmarkQueryHandler queryHandler = new BenchmarkQueryHandler(
					connection.getQueryHandler()
			);

			// We'll need the documentIds from the database later on for the
			// evaluations.
			this.documentIds = Sets.newTreeSet(queryHandler.getDocumentIds());

			JSONObject stats = new JSONObject();

			int step = 1;

			logger.info("Step " + step + ": Running getBiGramsFromDocumentEvaluation");
			stats.put(
					"getBiGramsFromDocumentEvaluation",
					this.getBiGramsFromDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			step++;
			logger.info("Step " + step + ": Running getBiGramsFromAllDocumentsEvaluation");
			stats.put(
					"getBiGramsFromAllDocumentsEvaluation",
					this.getBiGramsFromAllDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			step++;
			logger.info("Step " + step + ": Running getBiGramsFromDocumentsInCollectionEvaluation");
			stats.put(
					"getBiGramsFromDocumentsInCollectionEvaluation",
					this.getBiGramsFromDocumentsInCollectionEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			step++;
			logger.info("Step " + step + ": Running getTriGramsFromDocumentEvaluation");
			stats.put(
					"getTriGramsFromDocumentEvaluation",
					this.getTriGramsFromDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			step++;
			logger.info("Step " + step + ": Running getTriGramsFromAllDocumentsEvaluation");
			stats.put(
					"getTriGramsFromAllDocumentsEvaluation",
					this.getTriGramsFromAllDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			step++;
			logger.info("Step " + step + ": Running getTriGramsFromDocumentsInCollectionEvaluation");
			stats.put(
					"getTriGramsFromDocumentsInCollectionEvaluation",
					this.getTriGramsFromDocumentsInCollectionEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			logger.info("Writing results...");
			// Write the results to a file
			outputProvider.writeJSON(
					AllComplexQueryEvaluationCase.class.getSimpleName(),
					this.dbName.toString() + "_" + inputFiles,
					stats
			);
			logger.info("AllComplexQueryEvaluationCase for Database \""
					+ this.dbName + "\" done.");
		}
	}

	/**
	 * Executes getBiGramsFromDocument for a random subsets of documentIds.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject getBiGramsFromDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		Set<String> randomDocumentIds = Collections.chooseSubset(
				this.documentIds,
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				results.put(
						documentId,
						queryHandler.getBiGramsFromDocument(documentId)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			}
		}

		JSONObject biGramStats = Formatting.createOutputForMethod(
				"getBiGramsFromDocument",
				queryHandler
		);
		biGramStats.getJSONObject("more").put(
				"comment",
				"Called for " + randomDocumentIds.size() + " different "
						+ "documents. See \"results\" for more details."
		);
		biGramStats.getJSONObject("more").put(
				"results", results
		);
		return biGramStats;
	}

	/**
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject getBiGramsFromAllDocumentsEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		Iterable<String> result = queryHandler.getBiGramsFromAllDocuments();

		JSONObject biGramStats = Formatting.createOutputForMethod(
				"getBiGramsFromAllDocuments",
				queryHandler
		);
		biGramStats.getJSONObject("more").put(
				"results", result
		);
		return biGramStats;
	}

	/**
	 * Executes getBiGramsFromDocumentsInCollection for multiple random subsets
	 * of documentIds.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject getBiGramsFromDocumentsInCollectionEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManySubsets = 20;
		int howManyDocuments = 20;

		JSONArray results = new JSONArray();

		for (int i = 0; i < howManySubsets; i++)
		{
			Set<String> randomDocumentIds = Collections.chooseSubset(
					this.documentIds,
					howManyDocuments
			);
			try
			{
				results.put(
						queryHandler.getBiGramsFromDocumentsInCollection(
								randomDocumentIds
						)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("A number of DocumentIds could "
						+ "not be found in the database, although they "
						+ "were there just a moment ago. Please check "
						+ "for concurrent access.");
			}
		}

		JSONObject biGramStats = Formatting.createOutputForMethod(
				"getBiGramsFromDocumentsInCollection",
				queryHandler
		);
		biGramStats.getJSONObject("more").put(
				"comment",
				"Called for " + howManySubsets + " different random subsets of"
						+ " at most " + howManyDocuments + "documents. See "
						+ "\"results\" for more details."
		);
		biGramStats.getJSONObject("more").put(
				"results", results
		);
		return biGramStats;
	}

	/**
	 * Executes getTriGramsFromDocument for a random subsets of documentIds.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject getTriGramsFromDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		Set<String> randomDocumentIds = Collections.chooseSubset(
				this.documentIds,
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				results.put(
						documentId,
						queryHandler.getTriGramsFromDocument(documentId)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			}
		}

		JSONObject triGramStats = Formatting.createOutputForMethod(
				"getTriGramsFromDocument",
				queryHandler
		);
		triGramStats.getJSONObject("more").put(
				"comment",
				"Called for " + randomDocumentIds.size() + " different "
						+ "documents. See \"results\" for more details."
		);
		triGramStats.getJSONObject("more").put(
				"results", results
		);
		return triGramStats;
	}

	/**
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject getTriGramsFromAllDocumentsEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		Iterable<String> result = queryHandler.getTriGramsFromAllDocuments();

		JSONObject triGramStats = Formatting.createOutputForMethod(
				"getTriGramsFromAllDocuments",
				queryHandler
		);
		triGramStats.getJSONObject("more").put(
				"results", result
		);
		return triGramStats;
	}

	/**
	 * Executes getBiGramsFromDocumentsInCollection for multiple random subsets
	 * of documentIds.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject getTriGramsFromDocumentsInCollectionEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManySubsets = 20;
		int howManyDocuments = 20;

		JSONArray results = new JSONArray();

		for (int i = 0; i < howManySubsets; i++)
		{
			Set<String> randomDocumentIds = Collections.chooseSubset(
					this.documentIds,
					howManyDocuments
			);
			try
			{
				results.put(
						queryHandler.getTriGramsFromDocumentsInCollection(
								randomDocumentIds
						)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("A number of DocumentIds could "
						+ "not be found in the database, although they "
						+ "were there just a moment ago. Please check "
						+ "for concurrent access.");
			}
		}

		JSONObject triGramStats = Formatting.createOutputForMethod(
				"getTriGramsFromDocumentsInCollection",
				queryHandler
		);
		triGramStats.getJSONObject("more").put(
				"comment",
				"Called for " + howManySubsets + " different random subsets of"
						+ " at most " + howManyDocuments + "documents. See "
						+ "\"results\" for more details."
		);
		triGramStats.getJSONObject("more").put(
				"results", results
		);
		return triGramStats;
	}

}
