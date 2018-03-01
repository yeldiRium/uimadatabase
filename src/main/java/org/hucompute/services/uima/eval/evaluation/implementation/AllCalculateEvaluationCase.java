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

import javax.naming.OperationNotSupportedException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

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
		connectionRequest.addRequestedConnection(MySQLConnection.class);
//		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		return connectionRequest;
	}

	@Override
	public void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	) throws IOException
	{
		int inputFiles = new File(System.getenv("INPUT_DIR")).list().length;
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

			int step = 1;

			// calculateTTRForAllDocuments
			logger.info("Step " + step + ": Running calculateTTRForAllDocumentsEvaluation");
			stats.put(
					"calculateTTRForAllDocumentsEvaluation",
					this.calculateTTRForAllDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateTTRForDocument
			step++;
			logger.info("Step " + step + ": Running calculateTTRForDocumentEvaluation");
			stats.put(
					"calculateTTRForDocumentEvaluation",
					this.calculateTTRForDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateTTRForCollectionOfDocuments
			step++;
			logger.info("Step " + step + ": Running calculateTTRForCollectionOfDocumentsEvaluation");
			stats.put(
					"calculateTTRForCollectionOfDocumentsEvaluation",
					this.calculateTTRForCollectionOfDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateRawTermFrequenciesInDocument
			step++;
			logger.info("Step " + step + ": Running calculateRawTermFrequenciesInDocumentEvaluation");
			stats.put(
					"calculateRawTermFrequenciesInDocumentEvaluation",
					this.calculateRawTermFrequenciesInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateRawTermFrequencyForLemmaInDocu
			// step++;ment
			logger.info("Step " + step + ": Running calculateRawTermFrequencyForLemmaInDocumentEvaluation");
			stats.put(
					"calculateRawTermFrequencyForLemmaInDocumentEvaluation",
					this.calculateRawTermFrequencyForLemmaInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateTermFrequencyWithDoubleNormFor
			// step++;LemmaInDocument
			logger.info("Step " + step + ": Running calculateTermFrequencyWithDoubleNormForLemmaInDocumentEvaluation");
			stats.put(
					"calculateTermFrequencyWithDoubleNormForLemmaInDocumentEvaluation",
					this.calculateTermFrequencyWithDoubleNormForLemmaInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateTermFrequencyWithLogNormForLem
			// step++;maInDocument
			logger.info("Step " + step + ": Running calculateTermFrequencyWithLogNormForLemmaInDocumentEvaluation");
			stats.put(
					"calculateTermFrequencyWithLogNormForLemmaInDocumentEvaluation",
					this.calculateTermFrequencyWithLogNormForLemmaInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateTermFrequenciesForLemmataInDoc
			// step++;ument
			logger.info("Step " + step + ": Running calculateTermFrequenciesForLemmataInDocumentEvaluation");
			stats.put(
					"calculateTermFrequenciesForLemmataInDocumentEvaluation",
					this.calculateTermFrequenciesForLemmataInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateInverseDocumentFrequency
			step++;
			logger.info("Step " + step + ": Running calculateInverseDocumentFrequencyEvaluation");
			stats.put(
					"calculateInverseDocumentFrequencyEvaluation",
					this.calculateInverseDocumentFrequencyEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateInverseDocumentFrequenciesForL
			// step++;emmataInDocument
			logger.info("Step " + step + ": Running calculateInverseDocumentFrequenciesForLemmataInDocumentEvaluation");
			stats.put(
					"calculateInverseDocumentFrequenciesForLemmataInDocumentEvaluation",
					this.calculateInverseDocumentFrequenciesForLemmataInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateTFIDFForLemmaInDocument
			step++;
			logger.info("Step " + step + ": Running calculateTFIDFForLemmaInDocumentEvaluation");
			stats.put(
					"calculateTFIDFForLemmaInDocumentEvaluation",
					this.calculateTFIDFForLemmaInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateTFIDFForLemmataInDocument
			step++;
			logger.info("Step " + step + ": Running calculateTFIDFForLemmataInDocumentEvaluation");
			stats.put(
					"calculateTFIDFForLemmataInDocumentEvaluation",
					this.calculateTFIDFForLemmataInDocumentEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			// calculateTFIDFForLemmataInAllDocuments
			step++;
			logger.info("Step " + step + ": Running calculateTFIDFForLemmataInAllDocumentsEvaluation");
			stats.put(
					"calculateTFIDFForLemmataInAllDocumentsEvaluation",
					this.calculateTFIDFForLemmataInAllDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			logger.info("Writing results...");
			// Write the results to a file
			outputProvider.writeJSON(
					AllCalculateEvaluationCase.class.getSimpleName(),
					this.dbName.toString() + "_" + inputFiles,
					stats
			);
			logger.info("AllCalculateEvaluationCase for Database \""
					+ this.dbName + "\" done.");
		}
	}

	/**
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
		Set<String> randomDocumentIds = Collections.chooseSubset(
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
			Set<String> randomDocumentIds = Collections.chooseSubset(
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
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	private JSONObject calculateRawTermFrequenciesInDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		Set<String> randomDocumentIds = Collections.chooseSubset(
				Sets.newTreeSet(this.documentIds), howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				results.put(
						documentId,
						queryHandler.calculateRawTermFrequenciesInDocument(
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
				"calculateRawTermFrequenciesInDocument",
				queryHandler
		);
		termFrequencyStats.getJSONObject("more").put(
				"comment",
				"Called for " + randomDocumentIds.size() + " random documents. "
						+ "See \"results\" for more details."
		);
		termFrequencyStats.getJSONObject("more").put(
				"results", results
		);
		return termFrequencyStats;
	}

	/**
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	private JSONObject calculateRawTermFrequencyForLemmaInDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		int howManyLemmata = 20;
		Set<String> randomDocumentIds = Collections.chooseSubset(
				Sets.newTreeSet(this.documentIds), howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				Set<String> randomLemmata = Collections.chooseSubset(
						queryHandler.getLemmataForDocument(documentId),
						howManyLemmata
				);
				JSONObject lemmaResults = new JSONObject();

				for (String lemma : randomLemmata)
				{
					double lemmaTF = queryHandler
							.calculateRawTermFrequencyForLemmaInDocument(
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
				"calculateRawTermFrequencyForLemmaInDocument",
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
		Set<String> randomDocumentIds = Collections.chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				Set<String> randomLemmata = Collections.chooseSubset(
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
		Set<String> randomDocumentIds = Collections.chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				Set<String> randomLemmata = Collections.chooseSubset(
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
		Set<String> randomDocumentIds = Collections.chooseSubset(
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
		Set<String> randomLemmata = Collections.chooseSubset(this.lemmata, howManyLemmata);

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
		Set<String> randomDocumentIds = Collections.chooseSubset(
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
		Set<String> randomDocumentIds = Collections.chooseSubset(
				Sets.newTreeSet(this.documentIds),
				howManyDocuments
		);

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				Set<String> randomLemmata = Collections.chooseSubset(
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
		Set<String> randomDocumentIds = Collections.chooseSubset(
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
