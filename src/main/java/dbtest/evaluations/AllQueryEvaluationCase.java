package dbtest.evaluations;

import com.google.common.collect.Sets;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.Connections;
import dbtest.connection.implementation.ArangoDBConnection;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.TypeHasNoValueException;
import dbtest.queryHandler.exceptions.TypeNotCountableException;
import dbtest.queryHandler.implementation.BenchmarkQueryHandler;
import dbtest.utility.Formatting;
import org.json.JSONObject;
import org.neo4j.helpers.collection.Iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static dbtest.utility.Collections.chooseSubset;

/**
 * Tests all purely query-related functionality. See the section 'Raw Querying'
 * in the QueryHandlerInterface, except all the store-methods and the 'populate-
 * CasWithDocument' method, since those are tested in the AllRead- and the All-
 * WriteEvaluationCase.
 * <p>
 * Expects data to exists in the database. Accomplish this by running it after
 * the AllWriteEvaluationCase.
 */
public class AllQueryEvaluationCase implements EvaluationCase
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
		connectionRequest.addRequestedConnection(ArangoDBConnection.class);
//		connectionRequest.addRequestedConnection(BaseXConnection.class);
//		connectionRequest.addRequestedConnection(CassandraConnection.class);
//		connectionRequest.addRequestedConnection(MongoDBConnection.class);
//		connectionRequest.addRequestedConnection(MySQLConnection.class);
		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		return connectionRequest;
	}

	/**
	 * Executes and benchmarks all purely query-related methods on
	 * QueryHandlers for each Connection supplied.
	 * This excludes the storage methods and the populateCasWithDocument method,
	 * since they are covered in AllWrite- and AllReadEvaluationCase.
	 *
	 * @param connectionResponse Contains all Connections requested in
	 *                           #requestConnection().
	 * @param outputProvider     The provider for outputting results.
	 * @throws IOException If an outputfile can not be created or written to.
	 */
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

			logger.info("Starting AllQueryEvaluationCase for Database \""
					+ this.dbName + "\".");

			JSONObject stats = new JSONObject();

			BenchmarkQueryHandler queryHandler = new BenchmarkQueryHandler(
					connection.getQueryHandler()
			);

			// 1. getDocumentIds
			logger.info("Step 1: run getDocumentIdsEvaluation");
			stats.put(
					"getDocumentIds",
					this.getDocumentIdsEvaluation(queryHandler)
			);
			logger.info("Step 1 done.");

			// 2. getLemmataForDocument
			logger.info("Step 2: Running getLemmataForDocumentEvaluation.");
			stats.put(
					"getLemmataForDocument",
					this.getLemmataForDocumentEvaluation(queryHandler)
			);
			logger.info("Step 2 done.");

			// 3. countDocumentsContainingLemma
			logger.info("Step 3: Running "
					+ "countDocumentsContainingLemmaEvaluation.");
			stats.put(
					"countDocumentsContainingLemma",
					this.countDocumentsContainingLemmaEvaluation(queryHandler)
			);
			logger.info("Step 3 done.");

			// 4. countElementsOfType
			logger.info("Step 4: Running countElementsOfTypeEvaluation.");
			for (ElementType type : ElementType.values())
			{
				logger.info("Step 4: Type - \"" + type + "\".");
				stats.put(
						"countElementsOfType-" + type,
						this.countElementsOfTypeEvaluation(queryHandler, type)
				);
			}
			logger.info("Step 4 done.");

			// 5. countElementsInDocumentOfType
			logger.info("Step 5: Running "
					+ "countElementsInDocumentOfTypeEvaluation.");
			int howManyDocuments = 20;
			Set<String> randomDocumentIds = chooseSubset(
					Sets.newHashSet(documentIds),
					howManyDocuments
			);
			for (ElementType type : new ElementType[]{
					ElementType.Paragraph,
					ElementType.Sentence,
					ElementType.Token,
					ElementType.Lemma})
			{
				logger.info("Step 5: Type - \"" + type + "\".");
				stats.put(
						"countElementsInDocumentOfType-" + type,
						this.countElementsInDocumentOfTypeEvaluation(
								queryHandler, type, randomDocumentIds
						)
				);
			}
			logger.info("Step 5 done.");

			// 6. countElementsOfTypeWithValue
			logger.info("Step 6: Running "
					+ "countElementsOfTypeWithValueEvaluation.");
			int howManyValues = 20;
			Set<String> randomValues = chooseSubset(lemmata, howManyValues);
			for (ElementType type : new ElementType[]{
					ElementType.Lemma,
					ElementType.Token,
					ElementType.Pos})
			{
				logger.info("Step 6: Type - \"" + type + "\".");
				stats.put(
						"countElementsOfTypeWithValue-" + type,
						this.countElementsOfTypeWithValueEvaluation(
								queryHandler, type, randomValues
						)
				);
			}
			logger.info("Step 6 done.");

			// 7. countElementsInDocumentOfTypeWithValue
			logger.info("Step 7: Running "
					+ "countElementsInDocumentOfTypeWithValueEvaluation.");
			for (ElementType type : new ElementType[]{
					ElementType.Lemma,
					ElementType.Token
			})
			{
				logger.info("Step 7: Type - \"" + type + "\".");
				stats.put(
						"countElementsInDocumentOfTypeWithValue-" + type,
						this.countElementsInDocumentOfTypeWithValue(
								queryHandler,
								type,
								randomDocumentIds,
								randomValues
						)
				);
			}
			logger.info("Step 7 done.");

			// 8. countOccurencesForEachLemmaInAllDocuments
			logger.info("Step 8: Running "
					+ "countOccurencesForEachLemmaInAllDocumentsEvaluation.");
			stats.put(
					"countOccurencesForEachLemmaInAllDocuments",
					this.countOccurencesForEachLemmaInAllDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step 8 done.");

			logger.info("Writing results...");
			// Write the results to a file
			outputProvider.writeJSON(
					AllQueryEvaluationCase.class.getName(),
					this.dbName.toString(),
					stats
			);
			logger.info("AllQueryEvaluationCase for Database \""
					+ this.dbName + "\" done.");
		}
	}

	/**
	 * 1.
	 * Queries all documentIds in the database and stores them as an Iterable
	 * for further use.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject getDocumentIdsEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		this.documentIds = queryHandler.getDocumentIds();
		JSONObject documentIdsStats = Formatting.createOutputForMethod(
				"getDocumentIds", queryHandler
		);
		documentIdsStats.getJSONObject("more").put(
				"documentsFound", Iterators.count(this.documentIds.iterator())
		);
		documentIdsStats.getJSONObject("more").put(
				"results",
				StreamSupport
						.stream(this.documentIds.spliterator(), false)
						.collect(Collectors.joining(", "))
		);
		return documentIdsStats;
	}

	/**
	 * 2.
	 * Queries for all lemmata across all documents found in step 1 and stores
	 * them as a Set for further use.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject getLemmataForDocumentEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		this.lemmata = new HashSet<>();
		for (String documentId : documentIds)
		{
			try
			{
				this.lemmata.addAll(
						queryHandler.getLemmataForDocument(documentId)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId " + documentId + " could " +
						"not be found in the database, although it was " +
						"there just a moment ago. Please check for " +
						"concurrent access.");
			}
		}
		JSONObject lemmataStats = Formatting.createOutputForMethod(
				"getLemmataForDocument",
				queryHandler
		);
		lemmataStats.getJSONObject("more").put(
				"comment", "Called method for each Document in the database."
		);
		lemmataStats.getJSONObject("more").put(
				"results",
				this.lemmata.stream().collect(Collectors.joining(", "))
		);
		return lemmataStats;
	}

	/**
	 * 3.
	 * This is executed for a random subset of the found lemmata from
	 * the second step.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject countDocumentsContainingLemmaEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyLemmata = 20;
		Set<String> randomLemmata = chooseSubset(lemmata, howManyLemmata);

		JSONObject results = new JSONObject();

		for (String lemma : randomLemmata)
		{
			results.put(
					lemma,
					queryHandler.countDocumentsContainingLemma(
							lemma
					)
			);
		}
		JSONObject countDocumentsContainingLemmaStats =
				Formatting.createOutputForMethod(
						"countDocumentsContainingLemma",
						queryHandler
				);
		countDocumentsContainingLemmaStats.getJSONObject("more").put(
				"comment", "Called method for " + randomLemmata.size()
						+ " random Lemmata. See \"lemmataSearched\"."
		);
		countDocumentsContainingLemmaStats.getJSONObject("more").put(
				"lemmataSearched", randomLemmata.parallelStream()
						.collect(Collectors.joining(", "))
		);
		countDocumentsContainingLemmaStats.getJSONObject("more").put(
				"results", results
		);
		return countDocumentsContainingLemmaStats;
	}

	/**
	 * 4.
	 * This is executed for each type and logged under fitting names.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @param type         The type for which the evaluation is executed.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject countElementsOfTypeEvaluation(
			BenchmarkQueryHandler queryHandler, ElementType type
	)
	{
		queryHandler.getMethodBenchmarks()
				.get("countElementsOfType").reset();

		try
		{
			int count = queryHandler.countElementsOfType(type);
			JSONObject countElementsOfTypeStats =
					Formatting.createOutputForMethod(
							"countElementsOfType",
							queryHandler
					);
			countElementsOfTypeStats.getJSONObject("more")
					.put(
							"comment",
							"Called for type \"" + type + "\"."
					);
			countElementsOfTypeStats.getJSONObject("more")
					.put(
							"results",
							count
					);
			return countElementsOfTypeStats;
		} catch (TypeNotCountableException e)
		{
			JSONObject error = new JSONObject();
			error.put(
					"method", "countElementsOfType"
			);
			error.put(
					"error", "Elements of type \"" + type + "\" could not "
							+ "be counted."
			);
			return error;
		}
	}

	/**
	 * 5.
	 * This is executed for each type that can be contained in a docu-
	 * ment (Paragraph, Sentence, Token, Lemma) on a random subset of
	 * the found documents from step one.
	 * If this is called for the types Document or Pos the result may not be
	 * predictable.
	 *
	 * @param queryHandler      The QueryHandler on which the evaluation is
	 *                          performed.
	 * @param type              The type for which to execute the evaluation.
	 * @param randomDocumentIds The set of document to evaluate on.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject countElementsInDocumentOfTypeEvaluation(
			BenchmarkQueryHandler queryHandler,
			ElementType type,
			Set<String> randomDocumentIds
	)
	{
		queryHandler.getMethodBenchmarks()
				.get("countElementsInDocumentOfType").reset();

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			try
			{
				results.put(
						documentId,
						queryHandler.countElementsInDocumentOfType(
								documentId, type
						)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			} catch (TypeNotCountableException e)
			{
				logger.warning("QueryHandler for db " + dbName
						+ " was not able to count elements of type \""
						+ type + "\".");
				JSONObject error = new JSONObject();
				error.put("method", "countElementsInDocumentOfType");
				error.put("error", "Elements of type \"" + type + "\" could"
						+ " not be counted.");
				return error;
			}
		}

		JSONObject countElementsInDocumentOfTypeStats =
				Formatting.createOutputForMethod(
						"countElementsInDocumentOfType",
						queryHandler
				);
		countElementsInDocumentOfTypeStats.getJSONObject("more").put(
				"comment",
				"Called method for type \"" + type + "\" on "
						+ randomDocumentIds.size() + " random "
						+ "Documents. See \""
						+ "documentsSearched\"."
		);
		countElementsInDocumentOfTypeStats.getJSONObject("more").put(
				"documentsSearched",
				randomDocumentIds.parallelStream()
						.collect(Collectors.joining(", "))
		);
		countElementsInDocumentOfTypeStats.getJSONObject("more").put(
				"results", results
		);
		return countElementsInDocumentOfTypeStats;
	}

	/**
	 * 6.
	 * This is executed for each type that can have a value on a random
	 * subset of the found lemmata, since they correspond to their
	 * values as well as the tokens' values.
	 * The TypeHasNoValueException should never occur (this is not data-
	 * basedependent), unless someone made a mistake in a QueryHandler-
	 * implementation or in calling this method.
	 * <p>
	 * There is currently no way to test this for Pos elements, so the
	 * statistics for it will return 0 for everything.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @param type         The type for which to execute the evaluation.
	 * @param randomValues The set of values to evaluate on.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	protected JSONObject countElementsOfTypeWithValueEvaluation(
			BenchmarkQueryHandler queryHandler,
			ElementType type,
			Set<String> randomValues
	)
	{
		queryHandler.getMethodBenchmarks()
				.get("countElementsOfTypeWithValue").reset();

		JSONObject results = new JSONObject();

		for (String value : randomValues)
		{
			try
			{
				results.put(
						value,
						queryHandler.countElementsOfTypeWithValue(type, value)
				);
			} catch (TypeNotCountableException e)
			{
				JSONObject error = new JSONObject();
				error.put(
						"method", "countElementsOfTypeWithValue"
				);
				error.put(
						"error", "Elements of type \"" + type + "\" could "
								+ "not be counted."
				);
				return error;
			} catch (TypeHasNoValueException e)
			{
				logger.severe("DB " + dbName + " stated, that type "
						+ type + " does not have a value. This must "
						+ " be a programming error, since that should "
						+ " not be the case.");
				JSONObject error = new JSONObject();
				error.put(
						"method", "countElementsOfTypeWithValue"
				);
				error.put(
						"error", "Elements of type \"" + type + "\" don't have "
								+ "a value. Please check this, this should not "
								+ "happen."
				);
				return error;
			}
		}

		JSONObject countElementsOfTypeWithValueStats =
				Formatting.createOutputForMethod(
						"countElementsOfTypeWithValue",
						queryHandler
				);
		countElementsOfTypeWithValueStats.getJSONObject("more").put(
				"comment",
				"Called method for type \"" + type.toString() + "\" on "
						+ randomValues.size() + " different values. "
						+ "See \"searchedValues\"."
		);
		countElementsOfTypeWithValueStats.getJSONObject("more").put(
				"searchedValues",
				randomValues.parallelStream().collect(
						Collectors.joining(", ")
				)
		);
		countElementsOfTypeWithValueStats.getJSONObject("more").put(
				"results", results
		);
		return countElementsOfTypeWithValueStats;
	}

	/**
	 * 7.
	 * Executed for every type that has a value and is connected to a document.
	 * If the TypeHasNoValueException ever occurs here, the regarding QueryHand-
	 * ler is implemented wrong or this method is called with an incompatible
	 * type.
	 *
	 * @param queryHandler      The QueryHandler on which the evaluation is per-
	 *                          formed.
	 * @param type              The type for which to execute the evaluation.
	 * @param randomDocumentIds The set of document to evaluate on.
	 * @param randomValues      The set of values to evaluate on.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	private JSONObject countElementsInDocumentOfTypeWithValue(
			BenchmarkQueryHandler queryHandler,
			ElementType type,
			Set<String> randomDocumentIds,
			Set<String> randomValues
	)
	{
		queryHandler.getMethodBenchmarks()
				.get("countElementsInDocumentOfTypeWithValue").reset();

		JSONObject results = new JSONObject();

		for (String documentId : randomDocumentIds)
		{
			JSONObject docResults = new JSONObject();
			for (String value : randomValues)
			{
				try
				{
					docResults.put(
							value, queryHandler.countElementsInDocumentOfTypeWithValue(
									documentId, type, value
							)
					);
				} catch (TypeNotCountableException e)
				{
					JSONObject error = new JSONObject();
					error.put(
							"method", "countElementsInDocumentOfTypeWithValue"
					);
					error.put(
							"error", "Elements of type \"" + type + "\" could "
									+ "not be counted."
					);
					return error;
				} catch (TypeHasNoValueException e)
				{
					logger.severe("DB \"" + this.dbName + "\" stated, that "
							+ "type \"" + type + "\" does not have a value. "
							+ "This must  be a programming error, since that "
							+ "should  not be the case.");
					JSONObject error = new JSONObject();
					error.put(
							"method", "countElementsInDocumentOfTypeWithValue"
					);
					error.put(
							"error", "Elements of type \"" + type + "\" don't"
									+ " have a value. Please check this, this "
									+ "should not happen."
					);
					return error;
				} catch (DocumentNotFoundException e)
				{
					logger.warning("DocumentId \"" + documentId + "\" could "
							+ "not be found in the database, although it "
							+ "was there just a moment ago. Please check "
							+ "for concurrent access.");
					break;
				}
			}
			results.put(
					documentId,
					docResults
			);
		}

		JSONObject countElementsInDocumentOfTypeWithValueStats =
				Formatting.createOutputForMethod(
						"countElementsInDocumentOfTypeWithValue",
						queryHandler
				);
		countElementsInDocumentOfTypeWithValueStats.getJSONObject("more").put(
				"comment",
				"Called method for type \"" + type.toString() + "\" on "
						+ randomValues.size() + " different values in "
						+ randomDocumentIds.size() + " different "
						+ "documents. See \"searchedValues\" and "
						+ "\"searchedDocuments\"."
		);
		countElementsInDocumentOfTypeWithValueStats.getJSONObject("more").put(
				"searchedValues",
				randomValues.parallelStream().collect(
						Collectors.joining(", ")
				)
		);
		countElementsInDocumentOfTypeWithValueStats.getJSONObject("more").put(
				"searchedDocuments",
				randomDocumentIds.parallelStream().collect(
						Collectors.joining(", ")
				)
		);
		countElementsInDocumentOfTypeWithValueStats.getJSONObject("more").put(
				"results", results
		);
		return countElementsInDocumentOfTypeWithValueStats;
	}

	/**
	 * 8.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	private JSONObject countOccurencesForEachLemmaInAllDocumentsEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		Map<String, Integer> results =
				queryHandler.countOccurencesForEachLemmaInAllDocuments();

		JSONObject stats = Formatting.createOutputForMethod(
				"countOccurencesForEachLemmaInAllDocuments",
				queryHandler
		);
		stats.getJSONObject("more").put(
				"results", results
		);
		return stats;
	}
}
