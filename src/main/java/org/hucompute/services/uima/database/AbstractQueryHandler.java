package org.hucompute.services.uima.database;

import org.hucompute.services.uima.database.neo4j.data.Const;
import org.json.JSONObject;

import java.util.ArrayDeque;
import java.util.NoSuchElementException;

/**
 * QueryHandler abstract superclass. Interprets querys and calls functions.
 *
 * @author Manuel Stoeckel
 * Created on 26.09.2017
 */
public abstract class AbstractQueryHandler implements QueryHandlerInterface
{

	/**
	 * The first argument is the function name, others are their respective arguments.
	 * The function name is removed from the Collection via .pop() leaving only function-arguments or an empty collection.
	 * <p>In case of a _collection call the entire ArrayDeque will be passed as argument.
	 * Else, function-arguments are filled using .pop().</p>
	 *
	 * @param query ArrayDeque (Collection) containing all arguments from a single query.
	 * @return a JSON-Response-Object.
	 * @throws UnsupportedOperationException when the name of the function is unknown.
	 * @throws NoSuchElementException        when the number of arguments is not enough to call the function.
	 * @throws IllegalArgumentException      when an argument should be in Const.TYPE, but is not.
	 */
	public JSONObject interpret(ArrayDeque<String> query)
			throws UnsupportedOperationException, NoSuchElementException, IllegalArgumentException
	{
		JSONObject output = new JSONObject();
		try
		{
			String func = query.pop();
			switch (func)
			{
				case "calculateTTRForAllDocuments":
				{
					output.put("calculateTTRForAllDocuments", calculateTTRForAllDocuments());
					break;
				}
				case "calculateTTRForDocument":
				{
					output.put("calculateTTRForDocument", calculateTTRForDocument(query.pop()));
					break;
				}
				case "calculateTTRForCollectionOfDocuments":
				{
					output.put("calculateTTRForCollectionOfDocuments", calculateTTRForCollectionOfDocuments(query));
					break;
				}
				case "countElementsOfType":
				{
					output.put("countElementsOfType", countElementsOfType(Const.TYPE.valueOf(query.pop().toUpperCase())));
					break;
				}
				case "countElementsInDocumentOfType":
				{
					output.put("countElementsInDocumentOfType", countElementsInDocumentOfType(
							query.pop(), Const.TYPE.valueOf(query.pop().toUpperCase())));
					break;
				}
				case "countElementsOfTypeWithValue":
				{
					output.put("countElementsOfTypeWithValue", countElementsOfTypeWithValue(
							Const.TYPE.valueOf(query.pop().toUpperCase()), query.pop()));
					break;
				}
				case "countElementsInDocumentOfTypeWithValue":
				{
					output.put("countElementsInDocumentOfTypeWithValue", countElementsInDocumentOfTypeWithValue(
							query.pop(), Const.TYPE.valueOf(query.pop().toUpperCase()), query.pop()));
					break;
				}
				case "count_tvd":
				{
					output.put("countElementsInDocumentOfTypeWithValue", countElementsInDocumentOfTypeWithValue(
							query.pop(), Const.TYPE.valueOf(query.pop().toUpperCase()), query.pop()));
					break;
				}
				case "calculateTermFrequencyWithDoubleNormInDocumentForLemma":
				{
					output.put("calculateTermFrequencyWithDoubleNormInDocumentForLemma", calculateTermFrequencyWithDoubleNormForLemmaInDocument(query.pop(), query.pop()));
					break;
				}
				case "calculateTermFrequencyWithLogNermInDocumentForLemma":
				{
					output.put("calculateTermFrequencyWithLogNermInDocumentForLemma", calculateTermFrequencyWithLogNermForLemmaInDocument(query.pop(), query.pop()));
					break;
				}
				case "calculateTermFrequenciesForLemmataInDocument":
				{
					output.put("calculateTermFrequenciesForLemmataInDocument", calculateTermFrequenciesForLemmataInDocument(query.pop()));
					break;
				}
				case "countDocumentsContainingLemma":
				{
					output.put("countDocumentsContainingLemma", countDocumentsContainingLemma(query.pop()));
					break;
				}
				case "calculateInverseDocumentFrequency":
				{
					output.put("calculateInverseDocumentFrequency", calculateInverseDocumentFrequency(query.pop()));
					break;
				}
				case "calculateInverseDocumentFrequenciesForLemmataInDocument":
				{
					output.put("calculateInverseDocumentFrequenciesForLemmataInDocument", calculateInverseDocumentFrequenciesForLemmataInDocument(query.pop()));
					break;
				}
				case "calculateTFIDFForLemmaInDocument":
				{
					output.put("calculateTFIDFForLemmaInDocument", calculateTFIDFForLemmaInDocument(query.pop(), query.pop()));
					break;
				}
				case "calculateTFIDFForLemmataInDocument":
				{
					output.put("calculateTFIDFForLemmataInDocument", calculateTFIDFForLemmataInDocument(query.pop()));
					break;
				}
				case "calculateTFIDFForLemmataInAllDocuments":
				{
					output.put("calculateTFIDFForLemmataInAllDocuments", calculateTFIDFForLemmataInAllDocuments());
					break;
				}
				case "getLemmataForDocument":
				{
					output.put("getLemmataForDocument", getLemmataForDocument(query.pop()));
					break;
				}
				case "getBiGramsFromDocument":
				{
					output.put("getBiGramsFromDocument", getBiGramsFromDocument(query.pop()));
					break;
				}
				case "getBiGramsFromDocumentsInCollection":
				{
					output.put("getBiGramsFromDocumentsInCollection", getBiGramsFromDocumentsInCollection(query));
					break;
				}
				case "getBiGramsFromAllDocuments":
				{
					output.put("getBiGramsFromAllDocuments", getBiGramsFromAllDocuments());
					break;
				}
				case "getTriGramsFromDocument":
				{
					output.put("getTriGramsFromDocument", getTriGramsFromDocument(query.pop()));
					break;
				}
				case "getTriGramsFromDocumentsInCollection":
				{
					output.put("getTriGramsFromDocumentsInCollection", getTriGramsFromDocumentsInCollection(query));
					break;
				}
				case "getTriGramsFromAllDocuments":
				{
					output.put("getTriGramsFromAllDocuments", getTriGramsFromAllDocuments());
					break;
				}
				default:
				{
					throw new UnsupportedOperationException(func + " is not a valid function name!");
				}
			}
		} catch (Exception e)
		{
			if (e.getClass() == UnsupportedOperationException.class)
			{
				e.printStackTrace();
			} else if (e.getClass() == IllegalArgumentException.class)
			{
				e.printStackTrace();
			} else if (e.getClass() == NoSuchElementException.class)
			{
				e.printStackTrace();
			} else
			{
				System.out.println("Unexpected exception!");
				e.printStackTrace();
			}
		}
		return output;
	}
}
