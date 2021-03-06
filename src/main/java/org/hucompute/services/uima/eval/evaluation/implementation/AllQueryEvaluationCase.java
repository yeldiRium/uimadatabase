package org.hucompute.services.uima.eval.evaluation.implementation;

import com.google.common.collect.Sets;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.QueryHandlerInterface;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;
import org.hucompute.services.uima.eval.database.abstraction.implementation.BenchmarkQueryHandler;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;
import org.hucompute.services.uima.eval.evaluation.framework.OutputProvider;
import org.hucompute.services.uima.eval.utility.Collections;
import org.hucompute.services.uima.eval.utility.Formatting;
import org.json.JSONObject;
import org.neo4j.helpers.collection.Iterators;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

	/**
	 * Executes and benchmarks all purely query-related methods on
	 * QueryHandlers for each Connection supplied.
	 * This excludes the storage methods and the populateCasWithDocument method,
	 * since they are covered in AllWrite- and AllReadEvaluationCase.
	 *
	 * @param connectionResponse Contains all Connections requested in
	 *                           #requestConnection().
	 * @param queryHandlers
	 * @param outputProvider     The provider for outputting results.
	 * @throws IOException If an outputfile can not be created or written to.
	 */
	@Override
	public void run(
			Collection<QueryHandlerInterface> queryHandlers,
			OutputProvider outputProvider
	) throws IOException
	{
		int inputFiles = new File(System.getenv("INPUT_DIR")).list().length;
		for (QueryHandlerInterface currentQueryHandler : queryHandlers)
		{
			this.dbName = currentQueryHandler.forConnection();

			logger.info("Starting AllQueryEvaluationCase for Database \""
					+ this.dbName + "\".");

			JSONObject stats = new JSONObject();

			BenchmarkQueryHandler queryHandler = new BenchmarkQueryHandler(
					currentQueryHandler
			);
			queryHandler.openDatabase();

			int step = 1;
			// getDocumentIds
			logger.info("Step " + step + ": run getDocumentIdsEvaluation");
			stats.put(
					"getDocumentIds",
					this.getDocumentIdsEvaluation(queryHandler)
			);
			logger.info("Step " + step + " done.");

			// checkIfDocumentExists
			step++;
			logger.info("Step " + step + ": Running checkIfDocumentExistsEvaluation.");
			stats.put(
					"checkIfDocumentExists",
					this.checkIfDocumentExistsEvaluation(queryHandler)
			);
			logger.info("Step " + step + " done.");

			// getLemmataForDocument
			step++;
			logger.info("Step " + step + ": Running getLemmataForDocumentEvaluation.");
			stats.put(
					"getLemmataForDocument",
					this.getLemmataForDocumentEvaluation(queryHandler)
			);
			logger.info("Step " + step + " done.");

			// countDocumentsContainingLemma
			step++;
			logger.info("Step " + step + ": Running "
					+ "countDocumentsContainingLemmaEvaluation.");
			stats.put(
					"countDocumentsContainingLemma",
					this.countDocumentsContainingLemmaEvaluation(queryHandler)
			);
			logger.info("Step " + step + " done.");

			// countElementsOfType
			step++;
			logger.info("Step " + step + ": Running countElementsOfTypeEvaluation.");
			for (ElementType type : ElementType.values())
			{
				logger.info("Step " + step + ": Type - \"" + type + "\".");
				stats.put(
						"countElementsOfType-" + type,
						this.countElementsOfTypeEvaluation(queryHandler, type)
				);
			}
			logger.info("Step " + step + " done.");

			// countElementsInDocumentOfType
			step++;
			logger.info("Step " + step + ": Running "
					+ "countElementsInDocumentOfTypeEvaluation.");
			int howManyDocuments = 20;
			Set<String> randomDocumentIds = Collections.chooseSubset(
					Sets.newHashSet(documentIds),
					howManyDocuments
			);
			for (ElementType type : new ElementType[]{
					ElementType.Paragraph,
					ElementType.Sentence,
					ElementType.Token,
					ElementType.Lemma})
			{
				logger.info("Step " + step + ": Type - \"" + type + "\".");
				stats.put(
						"countElementsInDocumentOfType-" + type,
						this.countElementsInDocumentOfTypeEvaluation(
								queryHandler, type, randomDocumentIds
						)
				);
			}
			logger.info("Step " + step + " done.");

			// countElementsOfTypeWithValue
			step++;
			logger.info("Step " + step + ": Running "
					+ "countElementsOfTypeWithValueEvaluation.");
			int howManyValues = 20;
			Set<String> randomValues = Collections.chooseSubset(lemmata, howManyValues);
			for (ElementType type : new ElementType[]{
					ElementType.Lemma,
					ElementType.Token,
					ElementType.Pos})
			{
				logger.info("Step " + step + ": Type - \"" + type + "\".");
				stats.put(
						"countElementsOfTypeWithValue-" + type,
						this.countElementsOfTypeWithValueEvaluation(
								queryHandler, type, randomValues
						)
				);
			}
			logger.info("Step " + step + " done.");

			// countElementsInDocumentOfTypeWithValue
			step++;
			logger.info("Step " + step + ": Running "
					+ "countElementsInDocumentOfTypeWithValueEvaluation.");
			for (ElementType type : new ElementType[]{
					ElementType.Lemma,
					ElementType.Token
			})
			{
				logger.info("Step " + step + ": Type - \"" + type + "\".");
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
			logger.info("Step " + step + " done.");

			// countOccurencesForEachLemmaInAllDocuments
			step++;
			logger.info("Step " + step + ": Running "
					+ "countOccurencesForEachLemmaInAllDocumentsEvaluation.");
			stats.put(
					"countOccurencesForEachLemmaInAllDocuments",
					this.countOccurencesForEachLemmaInAllDocumentsEvaluation(
							queryHandler
					)
			);
			logger.info("Step " + step + " done.");

			logger.info("Writing results...");
			// Write the results to a file
			outputProvider.writeJSON(
					AllQueryEvaluationCase.class.getSimpleName(),
					this.dbName.toString() + "_" + inputFiles,
					stats
			);
			logger.info("AllQueryEvaluationCase for Database \""
					+ this.dbName + "\" done.");
		}
	}

	/**
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
	 * Checks for a few documents, if they exist.
	 *
	 * @param queryHandler The QueryHandler on which the evaluation is perfor-
	 *                     med.
	 * @return A JSONObject with stats regarding the evaluation.
	 */
	private JSONObject checkIfDocumentExistsEvaluation(
			BenchmarkQueryHandler queryHandler
	)
	{
		int howManyDocuments = 20;
		Set<String> randomDocumentIds = Collections.chooseSubset(
				Sets.newTreeSet(this.documentIds), howManyDocuments
		);

		int notFoundCounter = 0;
		for (String documentId : randomDocumentIds)
		{
			try
			{
				queryHandler.checkIfDocumentExists(documentId);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could "
						+ "not be found in the database, although it "
						+ "was there just a moment ago. Please check "
						+ "for concurrent access.");
				notFoundCounter++;
			}
		}

		JSONObject checkStats = Formatting.createOutputForMethod(
				"checkIfDocumentExists",
				queryHandler
		);
		checkStats.getJSONObject("more").put(
				"comment", "Checked the existence of " + randomDocumentIds
						.size() + " random documents."
		);
		checkStats.getJSONObject("more").put(
				"results", notFoundCounter + " documents were NOT found."
		);
		return checkStats;
	}

	/**
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
		Set<String> randomLemmata = Collections.chooseSubset(lemmata, howManyLemmata);

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
