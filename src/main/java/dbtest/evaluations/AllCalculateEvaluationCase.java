package dbtest.evaluations;

import com.google.common.collect.Sets;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.Connections;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.implementation.BenchmarkQueryHandler;
import dbtest.utility.Formatting;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import static dbtest.utility.Collections.chooseSubset;

/**
 * Evaluates the times for executing calculation-related methods on QueryHand-
 * lers.
 * <p>
 * Expects data to exists in the database. Accomplish this by running it after
 * the AllWriteEvaluationCase.
 */
public class AllCalculateEvaluationCase implements EvaluationCase
{
	protected static final Logger logger =
			Logger.getLogger(AllQueryEvaluationCase.class.getName());

	protected Connections.DBName dbName;
	protected Iterable<String> documentIds;
	protected Set<String> lemmata;

	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
//		connectionRequest.addRequestedConnection(ArangoDBConnection.class);
//		connectionRequest.addRequestedConnection(BaseXConnection.class);
//		connectionRequest.addRequestedConnection(CassandraConnection.class);
//		connectionRequest.addRequestedConnection(MongoDBConnection.class);
//		connectionRequest.addRequestedConnection(MySQLConnection.class);
		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		return connectionRequest;
	}

	@Override
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	) throws IOException
	{
		for (Connection connection : connectionResponse.getConnections())
		{
			this.dbName =
					Connections.getIdentifierForConnectionClass(
							connection.getClass()
					);

			logger.info("Starting AllCalculateEvaluationCase for Database \""
					+ this.dbName + "\".");

			BenchmarkQueryHandler queryHandler = new BenchmarkQueryHandler(
					connection.getQueryHandler()
			);

			// We'll need the documentIds and lemmata from the database later on
			// for the evaluations.
			this.documentIds = queryHandler.getDocumentIds();
			this.lemmata = new TreeSet<>();
			this.documentIds.forEach(documentId -> {
				try
				{
					this.lemmata.addAll(
							queryHandler.getLemmataForDocument(documentId)
					);
				} catch (DocumentNotFoundException e)
				{
					logger.warning("DocumentId \"" + documentId + "\" could "
							+ "not be found in the database, although it "
							+ "was there just a moment ago. Please check "
							+ "for concurrent access.");
				}
			});

			JSONObject stats = new JSONObject();

			// 1. calculateTTRForAllDocuments
			logger.info("Step 1: Running calculateTTRForAllDocumentsEvaluation");
			stats.put(
					"calculateTTRForAllDocumentsEvaluation",
					this.calculateTTRForAllDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step 1 done.");

			// 2. calculateTTRForDocument
			logger.info("Step 2: Running calculateTTRForDocumentEvaluation");
			stats.put(
					"calculateTTRForDocumentEvaluation",
					this.calculateTTRForDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step 2 done.");

			// 3. calculateTTRForCollectionOfDocuments
			logger.info("Step 3: Running calculateTTRForCollectionOfDocumentsEvaluation");
			stats.put(
					"calculateTTRForCollectionOfDocumentsEvaluation",
					this.calculateTTRForCollectionOfDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step 3 done.");

			// 4. calculateTermFrequencyWithDoubleNormForLemmaInDocument
			logger.info("Step 4: Running calculateTermFrequencyWithDoubleNormForLemmaInDocumentEvaluation");
			stats.put(
					"calculateTermFrequencyWithDoubleNormForLemmaInDocumentEvaluation",
					this.calculateTermFrequencyWithDoubleNormForLemmaInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step 4 done.");

			// 5. calculateTermFrequencyWithLogNormForLemmaInDocument
			logger.info("Step 5: Running calculateTermFrequencyWithLogNormForLemmaInDocumentEvaluation");
			stats.put(
					"calculateTermFrequencyWithLogNormForLemmaInDocumentEvaluation",
					this.calculateTermFrequencyWithLogNormForLemmaInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step 5 done.");

			// 6. calculateTermFrequenciesForLemmataInDocument
			logger.info("Step 6: Running calculateTermFrequenciesForLemmataInDocumentEvaluation");
			stats.put(
					"calculateTermFrequenciesForLemmataInDocumentEvaluation",
					this.calculateTermFrequenciesForLemmataInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step 6 done.");

			// 7. calculateInverseDocumentFrequency
			logger.info("Step 7: Running calculateInverseDocumentFrequencyEvaluation");
			stats.put(
					"calculateInverseDocumentFrequencyEvaluation",
					this.calculateInverseDocumentFrequencyEvaluation(
							queryHandler
					)
			);
			logger.info("Step 7 done.");

			// 8. calculateInverseDocumentFrequenciesForLemmataInDocument
			logger.info("Step 8: Running calculateInverseDocumentFrequenciesForLemmataInDocumentEvaluation");
			stats.put(
					"calculateInverseDocumentFrequenciesForLemmataInDocumentEvaluation",
					this.calculateInverseDocumentFrequenciesForLemmataInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step 8 done.");

			// 9. calculateTFIDFForLemmaInDocument
			logger.info("Step 9: Running calculateTFIDFForLemmaInDocumentEvaluation");
			stats.put(
					"calculateTFIDFForLemmaInDocumentEvaluation",
					this.calculateTFIDFForLemmaInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step 9 done.");

			// 10. calculateTFIDFForLemmataInDocument
			logger.info("Step 10: Running calculateTFIDFForLemmataInDocumentEvaluation");
			stats.put(
					"calculateTFIDFForLemmataInDocumentEvaluation",
					this.calculateTFIDFForLemmataInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step 10 done.");

			// 11. calculateTFIDFForLemmataInAllDocuments
			logger.info("Step 11: Running calculateTFIDFForLemmataInAllDocumentsEvaluation");
			stats.put(
					"calculateTFIDFForLemmataInAllDocumentsEvaluation",
					this.calculateTFIDFForLemmataInAllDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step 11 done.");

			logger.info("Writing results...");
			// Write the results to a file
			outputProvider.writeJSON(
					AllCalculateEvaluationCase.class.getName(),
					this.dbName.toString(),
					stats
			);
			logger.info("AllCalculateEvaluationCase for Database \""
					+ this.dbName + "\" done.");
		}
	}

	/**
	 * 1.
	 * Calculates the TTR for all documents in the database.
	 * Outputs all found TTRs.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTTRForAllDocumentsEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		Map<String, Double> ttrs = queryHandler.calculateTTRForAllDocuments();
		JSONObject ttrStats = Formatting.createOutputForMethod(
				"calculateTTRForAllDocuments",
				queryHandler
		);
		ttrStats.getJSONObject("more").put(
				"results", new JSONObject(ttrs)
		);
		return ttrStats;
	}

	/**
	 * 2.
	 * Calculates the TTR for a randomly chosen subset of documentIds.
	 * Outputs all found TTRs.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTTRForDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		Set<String> randomDocumentIds = chooseSubset(
				Sets.newTreeSet(this.documentIds), howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				Double ttr = queryHandler.calculateTTRForDocument(documentId);
				results.put(documentId, ttr);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			}
		}

		JSONObject ttrStats = Formatting.createOutputForMethod(
				"calculateTTRForDocument",
				queryHandler
		);
		ttrStats.getJSONObject("more").put(
				"comment",
				"Called for " + randomDocumentIds.size() + " different random "
						+ "documents. See the keys in \"results\" for a list of"
						+ "the used documents."
		);
		ttrStats.getJSONObject("more").put(
				"results", results
		);
		return ttrStats;
	}

	/**
	 * 3.
	 * Calculates the TTR for multiple randomly chosen subsets of documentIds.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTTRForCollectionOfDocumentsEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManySubsets = 20;
		int howManyDocuments = 20;

		JSONArray results = new JSONArray();

		for (int i = 0; i < howManySubsets; i++)
		{
			Set<String> randomDocumentIds = chooseSubset(
					Sets.newTreeSet(this.documentIds), howManyDocuments
			);
			results.put(
					queryHandler.calculateTTRForCollectionOfDocuments(
							randomDocumentIds
					)
			);
		}

		JSONObject ttrStats = Formatting.createOutputForMethod(
				"calculateTTRForCollectionOfDocuments",
				queryHandler
		);
		ttrStats.getJSONObject("more").put(
				"comment",
				"Called for " + howManySubsets + " different random subsets of "
						+ "documents, each containing at most "
						+ howManyDocuments + " documents. See \"results\" for "
						+ "more details."
		);
		ttrStats.getJSONObject("more").put(
				"results", results
		);
		return ttrStats;
	}

	/**
	 * 4.
	 * Calculates the term frequency for a randomly chosen subset of documentIds
	 * and a randomly chosen subset of the lemmata in each of those documents
	 * using a double normalization.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTermFrequencyWithDoubleNormForLemmaInDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		int howManyLemmata = 20;
		Set<String> randomDocumentIds = chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				Set<String> randomLemmata = chooseSubset(
						queryHandler.getLemmataForDocument(documentId),
						howManyLemmata
				);
				JSONObject lemmaResults = new JSONObject();

				for (String lemma : randomLemmata)
				{
					double lemmaTF = queryHandler
							.calculateTermFrequencyWithDoubleNormForLemmaInDocument(
									lemma, documentId
							);
					lemmaResults.put(lemma, lemmaTF);
				}

				results.put(documentId, lemmaResults);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			}
		}

		JSONObject termFrequencyStats = Formatting.createOutputForMethod(
				"calculateTermFrequencyWithDoubleNormForLemmaInDocument",
				queryHandler
		);
		termFrequencyStats.getJSONObject("more").put(
				"comment",
				"Called for at most " + howManyLemmata + " random lemmata each "
						+ "on " + randomDocumentIds.size() + " documents. See "
						+ "\"results\" for more details."
		);
		termFrequencyStats.getJSONObject("more").put(
				"results", results
		);
		return termFrequencyStats;
	}

	/**
	 * 5.
	 * Calculates the term frequency for a randomly chosen subset of documentIds
	 * and a randomly chosen subset of the lemmata in each of those documents
	 * using a logarithmic normalization.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTermFrequencyWithLogNormForLemmaInDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		int howManyLemmata = 20;
		Set<String> randomDocumentIds = chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				Set<String> randomLemmata = chooseSubset(
						queryHandler.getLemmataForDocument(documentId),
						howManyLemmata
				);
				JSONObject lemmaResults = new JSONObject();

				for (String lemma : randomLemmata)
				{
					double lemmaTF = queryHandler
							.calculateTermFrequencyWithLogNormForLemmaInDocument(
									lemma, documentId
							);
					lemmaResults.put(lemma, lemmaTF);
				}

				results.put(documentId, lemmaResults);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			}
		}

		JSONObject termFrequencyStats = Formatting.createOutputForMethod(
				"calculateTermFrequencyWithLogNormForLemmaInDocument",
				queryHandler
		);
		termFrequencyStats.getJSONObject("more").put(
				"comment",
				"Called for at most " + howManyLemmata + " random lemmata each "
						+ "on " + randomDocumentIds.size() + " documents. See "
						+ "\"results\" for more details."
		);
		termFrequencyStats.getJSONObject("more").put(
				"results", results
		);
		return termFrequencyStats;
	}

	/**
	 * 6.
	 * Calculates the term frequency for a randomly chosen subset of documentIds
	 * using a double normalization.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTermFrequenciesForLemmataInDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		Set<String> randomDocumentIds = chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				results.put(
						documentId,
						queryHandler
								.calculateTermFrequenciesForLemmataInDocument(
										documentId
								)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			}
		}

		JSONObject termFrequencyStats = Formatting.createOutputForMethod(
				"calculateTermFrequenciesForLemmataInDocument",
				queryHandler
		);
		termFrequencyStats.getJSONObject("more").put(
				"comment",
				"Called for " + randomDocumentIds.size() + " documents. See "
						+ "\"results\" for more details."
		);
		termFrequencyStats.getJSONObject("more").put(
				"results", results
		);
		return termFrequencyStats;
	}

	/**
	 * 7.
	 * Calculates the inverse document frequency for a randomly chosen subset
	 * of all lemmata in the database.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateInverseDocumentFrequencyEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyLemmata = 100;
		Set<String> randomLemmata = chooseSubset(this.lemmata, howManyLemmata);

		JSONObject results = new JSONObject();

		for (String lemma : randomLemmata)
		{
			try
			{
				results.put(
						lemma,
						queryHandler.calculateInverseDocumentFrequency(lemma)
				);
			} catch (OperationNotSupportedException e)
			{
				return Formatting.createUnsupportedOperationError(
						"calculateInverseDocumentFrequency"
				);
			}
		}

		JSONObject inverseDocumentFrequencyStats = Formatting
				.createOutputForMethod(
						"calculateInverseDocumentFrequency",
						queryHandler
				);
		inverseDocumentFrequencyStats.getJSONObject("more").put(
				"comment",
				"Called for " + randomLemmata.size() + " lemmata. See "
						+ "\"results\" for more details."
		);
		inverseDocumentFrequencyStats.getJSONObject("more").put(
				"results", results
		);
		return inverseDocumentFrequencyStats;
	}

	/**
	 * 8.
	 * Calculates the inverse document frequency for each lemma on a randomly
	 * chosen subset of documentIds.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateInverseDocumentFrequenciesForLemmataInDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		Set<String> randomDocumentIds = chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				results.put(
						documentId,
						queryHandler.calculateInverseDocumentFrequenciesForLemmataInDocument(
								documentId
						)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			} catch (OperationNotSupportedException e)
			{
				return Formatting.createUnsupportedOperationError(
						"calculateInverseDocumentFrequenciesForLemmataInDocument"
				);
			}
		}

		JSONObject inverseDocumentFrequencyStats = Formatting
				.createOutputForMethod(
						"calculateInverseDocumentFrequenciesForLemmataInDocument",
						queryHandler
				);
		inverseDocumentFrequencyStats.getJSONObject("more").put(
				"comment",
				"Called for " + randomDocumentIds.size() + " documents. See "
						+ "\"results\" for more details."
		);
		inverseDocumentFrequencyStats.getJSONObject("more").put(
				"results", results
		);
		return inverseDocumentFrequencyStats;
	}

	/**
	 * 9.
	 * Calculates the tfidf for a randomly chosen subset of documentIds
	 * and a randomly chosen subset of the lemmata in each of those documents.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTFIDFForLemmaInDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		int howManyLemmata = 20;
		Set<String> randomDocumentIds = chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				Set<String> randomLemmata = chooseSubset(
						queryHandler.getLemmataForDocument(documentId),
						howManyLemmata
				);
				JSONObject lemmaResults = new JSONObject();

				for (String lemma : randomLemmata)
				{
					lemmaResults.put(
							lemma,
							queryHandler.calculateTFIDFForLemmaInDocument(
									lemma, documentId
							)
					);
				}

				results.put(
						documentId,
						lemmaResults
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			} catch (OperationNotSupportedException e)
			{
				return Formatting.createUnsupportedOperationError(
						"calculateTFIDFForLemmaInDocument"
				);
			}
		}

		JSONObject tfidfStats = Formatting
				.createOutputForMethod(
						"calculateTFIDFForLemmaInDocument",
						queryHandler
				);
		tfidfStats.getJSONObject("more").put(
				"comment",
				"Called for at most " + howManyLemmata + " lemmata on each of "
						+ randomDocumentIds.size() + " documents. See "
						+ "\"results\" for more details."
		);
		tfidfStats.getJSONObject("more").put(
				"results", results
		);
		return tfidfStats;
	}

	/**
	 * 10.
	 * Calculates the tfidf for a randomly chosen subset of document-
	 * Ids.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTFIDFForLemmataInDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		Set<String> randomDocumentIds = chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				results.put(
						documentId,
						queryHandler.calculateTFIDFForLemmataInDocument(documentId)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			} catch (OperationNotSupportedException e)
			{
				return Formatting.createUnsupportedOperationError(
						"calculateTFIDFForLemmataInDocument"
				);
			}
		}

		JSONObject tfidfStats = Formatting
				.createOutputForMethod(
						"calculateTFIDFForLemmataInDocument",
						queryHandler
				);
		tfidfStats.getJSONObject("more").put(
				"comment",
				"Called for " + randomDocumentIds.size() + " documents. See "
						+ "\"results\" for more details."
		);
		tfidfStats.getJSONObject("more").put(
				"results", results
		);
		return tfidfStats;
	}

	/**
	 * 11.
	 * Calculates the tfidf for all lemmata in the database.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject calculateTFIDFForLemmataInAllDocumentsEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		try
		{
			JSONObject results = new JSONObject(
					queryHandler.calculateTFIDFForLemmataInAllDocuments()
			);

			JSONObject tfidfStats = Formatting
					.createOutputForMethod(
							"calculateTFIDFForLemmataInAllDocuments",
							queryHandler
					);
			tfidfStats.getJSONObject("more").put(
					"results", results
			);
			return tfidfStats;
		} catch (OperationNotSupportedException e)
		{
			return Formatting.createUnsupportedOperationError(
					"calculateTFIDFForLemmataInAllDocuments"
			);
		}
	}
}
