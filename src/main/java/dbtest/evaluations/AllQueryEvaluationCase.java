package dbtest.evaluations;

import com.google.common.collect.Sets;
import dbtest.connection.Connection;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.Connections;
import dbtest.connection.implementation.*;
import dbtest.evaluationFramework.EvaluationCase;
import dbtest.evaluationFramework.OutputProvider;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.TypeHasNoValueException;
import dbtest.queryHandler.exceptions.TypeNotCountableException;
import dbtest.queryHandler.implementation.BenchmarkQueryHandler;
import org.json.JSONObject;
import org.neo4j.helpers.collection.Iterators;

import java.io.IOException;
import java.util.HashSet;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Expects data to exists in the database. Accomplish this by running it after
 * the AllWriteEvaluationCase.
 * <p>
 * Tests all purely query-related functionality. See the section 'Raw Querying'
 * in the QueryHandlerInterface, except all the store-methods and the 'populate-
 * CasWithDocument' method, since those are tested in the AllRead- and the All-
 * WriteEvaluationCase.
 */
public class AllQueryEvaluationCase implements EvaluationCase
{
	protected static final Logger logger =
			Logger.getLogger(AllQueryEvaluationCase.class.getName());

	protected static Random random = new Random();


	protected Connections.DBName dbName;
	protected Iterable<String> documentIds;
	protected Set<String> lemmata;

	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(ArangoDBConnection.class);
		connectionRequest.addRequestedConnection(BaseXConnection.class);
		connectionRequest.addRequestedConnection(CassandraConnection.class);
		connectionRequest.addRequestedConnection(MongoDBConnection.class);
		connectionRequest.addRequestedConnection(MySQLConnection.class);
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
			JSONObject stats = new JSONObject();

			BenchmarkQueryHandler queryHandler = new BenchmarkQueryHandler(
					connection.getQueryHandler()
			);

			this.dbName =
					Connections.getIdentifierForConnectionClass(
							connection.getClass()
					);

			// 1. getDocumentIds
			stats.put("getDocumentIds", getDocumentIdsEvaluation(queryHandler));

			// 2. getLemmataForDocument
			stats.put(
					"getLemmataForDocument",
					getLemmataForDocumentEvaluation(queryHandler)
			);

			// 3. countDocumentsContainingLemma
			stats.put(
					"countDocumentsContainingLemma",
					countDocumentsContainingLemmaEvaluation(queryHandler)
			);

			// 4. countElementsOfType
			for (ElementType type : ElementType.values())
			{
				stats.put(
						"countElementsOfType-" + type,
						countElementsOfTypeEvaluation(queryHandler, type)
				);
			}

			// 5. countElementsInDocumentOfType
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
				stats.put(
						"countElementsInDocumentOfType-" + type,
						countElementsInDocumentOfTypeEvaluation(
								queryHandler, type, randomDocumentIds
						)
				);
			}

			// 6. countElementsOfTypeWithValue
			int howManyValues = 20;
			Set<String> randomValues = chooseSubset(lemmata, howManyValues);
			for (ElementType type : new ElementType[]{
					ElementType.Lemma,
					ElementType.Token,
					ElementType.Pos})
			{
				stats.put(
						"countElementsOfTypeWithValue-" + type,
						countElementsOfTypeWithValueEvaluation(
								queryHandler, type, randomValues
						)
				);
			}
		}
	}

	/**
	 * 1.
	 * Stores all documentIds in the database as an Iterable.
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
		JSONObject documentIdsStats = this.createOutputForMethod(
				"getDocumentIds", queryHandler
		);
		documentIdsStats.getJSONObject("more").put(
				"documentsFound", Iterators.count(this.documentIds.iterator())
		);
		return documentIdsStats;
	}

	/**
	 * 2.
	 * Stores a set of all found lemmata in all documents returned in step 1.
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
		JSONObject lemmataStats = this.createOutputForMethod(
				"getLemmataForDocument",
				queryHandler
		);
		lemmataStats.getJSONObject("more").put(
				"comment", "called for each found documentId"
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
		for (String lemma : randomLemmata)
		{
			queryHandler.countDocumentsContainingLemma(
					lemma
			);
		}
		JSONObject countDocumentsContainingLemmaStats =
				this.createOutputForMethod(
						"countDocumentsContainingLemma",
						queryHandler
				);
		countDocumentsContainingLemmaStats.getJSONObject("more").put(
				"comment", "called for " + howManyLemmata + " random " +
						"lemmata, see \"lemmataSearched\""
		);
		countDocumentsContainingLemmaStats.getJSONObject("more").put(
				"lemmataSearched", randomLemmata.parallelStream()
						.collect(Collectors.joining(", "))
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
			queryHandler.countElementsOfType(type);
			JSONObject countElementsOfTypeStats =
					this.createOutputForMethod(
							"countElementsOfType",
							queryHandler
					);
			countElementsOfTypeStats.getJSONObject("more")
					.put(
							"comment",
							"called for type " + type
					);
			return countElementsOfTypeStats;
		} catch (TypeNotCountableException e)
		{
			JSONObject error = new JSONObject();
			error.put(
					"method", "countElementsOfType"
			);
			error.put(
					"error", "Elements of type " + type + " could not "
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
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @param type The type for which to execute the evaluation.
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

		for (String documentId : randomDocumentIds)
		{
			try
			{
				queryHandler.countElementsInDocumentOfType(
						documentId, type
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId " + documentId + " could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
			} catch (TypeNotCountableException e)
			{
				logger.warning("QueryHandler for db " + dbName
						+ " was not able to count elements of type "
						+ type + ".");
				JSONObject error = new JSONObject();
				error.put("method", "countElementsInDocumentOfType");
				error.put("error", "Elements of type " + type + " could"
						+ " not be counted.");
				return error;
			}
		}
		JSONObject countElementsInDocumentOfTypeStats =
				this.createOutputForMethod(
						"countElementsInDocumentOfType",
						queryHandler
				);
		countElementsInDocumentOfTypeStats.getJSONObject("more")
				.put(
						"comment",
						"called for type " + type + " on "
								+ randomDocumentIds.size() + " random "
								+ "documents. See \""
								+ "documentsSearched\"."
				);
		countElementsInDocumentOfTypeStats.getJSONObject("more")
				.put(
						"documentsSearched",
						randomDocumentIds.parallelStream()
								.collect(Collectors.joining(", "))
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
	 * implementation.

	 * There is currently no way to test this for Pos elements, so the
	 * statistics for it will return 0 for everything.
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @param type The type for which to execute the evaluation.
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

		for (String value : randomValues)
		{
			try
			{
				queryHandler.countElementsOfTypeWithValue(type, value);
			} catch (TypeNotCountableException e)
			{
				JSONObject error = new JSONObject();
				error.put(
						"method", "countElementsOfTypeWithValue"
				);
				error.put(
						"error", "Elements of type " + type + " could "
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
						"error", "Elements of type " + type + " don't have a "
								+ "value. Please check this, this should not "
								+ "happen."
				);
				return error;
			}
		}
		JSONObject countElementsOfTypeWithValueStats =
				this.createOutputForMethod(
						"countElementsOfTypeWithValue",
						queryHandler
				);
		countElementsOfTypeWithValueStats.getJSONObject("more")
				.put(
						"comment",
						"called for type " + type.toString() + " on "
								+ randomValues.size() + " different values. "
								+ "See \"searchedValues\"."
				);
		countElementsOfTypeWithValueStats.getJSONObject("more")
				.put(
						"searchedValues",
						randomValues.parallelStream().collect(
								Collectors.joining(", ")
						)
				);
		return countElementsOfTypeWithValueStats;
	}

	/**
	 * Creates generic statistics output for a given methodname.
	 * Calculates count, min, avg, max and sum and puts them into a JSONObject.
	 */
	protected JSONObject createOutputForMethod(
			String methodName,
			BenchmarkQueryHandler queryHandler
	)
	{
		LongSummaryStatistics stats = queryHandler.getMethodBenchmarks()
				.get(methodName).getCallTimes().parallelStream()
				.mapToLong(Long::longValue).summaryStatistics();
		JSONObject statsJSONObject = new JSONObject();

		statsJSONObject.put("method", methodName);
		statsJSONObject.put("callCount", stats.getCount());
		statsJSONObject.put("minTime", stats.getMin());
		statsJSONObject.put("avgTime", stats.getAverage());
		statsJSONObject.put("maxTime", stats.getMax());
		statsJSONObject.put("sumTime", stats.getSum());
		statsJSONObject.put("more", new JSONObject());
		return statsJSONObject;
	}

	/**
	 * Chooses a random subset of size `count` from the given set.
	 * If `count` is bigger than the set's size, the whole set is returned.
	 */
	protected static <E> Set<E> chooseSubset(Set<E> set, int count)
	{
		int n = set.size();
		if (count > n)
		{
			return set;
		}

		Set<E> result = new HashSet<>();
		E[] elements = (E[]) set.toArray();
		for (int i = 0; i < count; i++)
		{
			int k = random.nextInt(n);
			n--;
			E tmp = elements[n];
			elements[n] = elements[k];
			elements[k] = tmp;
			result.add(elements[n]);
		}
		return result;
	}
}
