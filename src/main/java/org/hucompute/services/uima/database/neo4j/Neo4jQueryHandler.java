package org.hucompute.services.uima.database.neo4j;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.database.AbstractQueryHandler;
import org.hucompute.services.uima.database.neo4j.data.Const;
import org.hucompute.services.uima.database.neo4j.impl.MDB_Neo4J_Impl;
import org.json.JSONObject;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * The Neo4j QueryHandler. Most queries can be solved with a single Cypher query, which is then transformed.
 * The transformed result is handed back to the {@link AbstractQueryHandler} {@link #interpret(ArrayDeque) interpret} function
 * where it is wrapped in a JSONObject. This JSON is handed back to the {@link org.hucompute.services.uima.database.RequestHandler RequestHandler}.
 *
 * @author Manuel Stoeckel
 * Created on 19.06.2017
 */
public class Neo4jQueryHandler extends AbstractQueryHandler
{

	private ArrayDeque<String> query;
	private Neo4jDriver driver;

	public Neo4jQueryHandler(String[] in)
	{
		this.query = new ArrayDeque<>();
		this.query.addAll(Arrays.asList(in));
		this.driver = new Neo4jDriver(getDbLink(null));
	}

	public Neo4jQueryHandler(ArrayDeque<String> in)
	{
		this.query = in;
		this.driver = new Neo4jDriver(getDbLink(null));
	}

	/**
	 * Concurrent call() function. Calls {@link AbstractQueryHandler} {@link #interpret(ArrayDeque) interpret} function.
	 * <p>Throws a generic {@link Exception} when driver instance cannot be closed for some reason.</p>
	 */
	@Override
	public JSONObject call()
	{
		JSONObject output = new JSONObject();
		output = interpret(this.query);

		try
		{
			driver.close();
		} catch (Exception e)
		{
			System.out.println("Driver could not be closed properly!");
			e.printStackTrace();
		}
		return output;
	}

	/*
	 * TODO:println->log, check for getTTR_opt
	 */
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		long startTimeJ = System.currentTimeMillis();
		Map<String, Double> ttr = new HashMap<>();
		try
		{
			long startTimeQ = System.currentTimeMillis();
			StatementResult result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)--(t:TOKEN)--(l:LEMMA) WITH d, count(DISTINCT l)*1.0/count(t) AS ttr SET d.ttr = ttr RETURN d.id, ttr ORDER BY d.id;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("Query took: " + (endTimeQ - startTimeQ) + " ms.");
			while (result.hasNext())
			{
				Record row = result.next();
				ttr.put(row.get("d.id").toString(), Double.parseDouble(row.get("ttr").toString()));
			}
		} catch (Exception e)
		{
			System.out.println("getTTR() query failed!");
			e.printStackTrace();
		} finally
		{
			long endTimeJ = System.currentTimeMillis();
			System.out.println("getTTR() took: " + (endTimeJ - startTimeJ) + " ms.");
		}
		return ttr;
	}

	public Map<String, Double> calculateTTRForDocument(String documentId)
	{
		return calculateTTRForCollectionOfDocuments(Arrays.asList(documentId));
	}

	/**
	 * Legacy function. Wraps array of Strings argument to {@link Collection}.
	 *
	 * @param documentIds the specified document's id.
	 * @return recursive call to {@link #calculateTTRForCollectionOfDocuments}.
	 */
	public Map<String, Double> getTTR(String documentIds[])
	{
		return calculateTTRForCollectionOfDocuments(Arrays.asList(documentIds));
	}

	public Map<String, Double> calculateTTRForCollectionOfDocuments(Collection<String> docIds)
	{
		long startTime = System.currentTimeMillis();
		ArrayList<String> documentIds = new ArrayList<>(docIds);
		if (documentIds == null || documentIds.isEmpty())
			return calculateTTRForAllDocuments();

		Map<String, Double> ttr = getTTR_opt(documentIds);
		if (ttr.keySet().containsAll(documentIds))
			return ttr;

		documentIds.removeAll(ttr.keySet());
		System.out.println("getTTR_opt didn't contain " + documentIds.toString() + "!");
		try
		{
			long startTimeQ = System.currentTimeMillis();
			StatementResult result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)--(t:TOKEN)--(l:LEMMA) WHERE d.id IN [" + documentIds.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + "] WITH d, count(DISTINCT l)*1.0/count(t) AS ttr RETURN d.id, ttr ORDER BY d.id;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("getTTR(ArrayList) query took: " + (endTimeQ - startTimeQ) + " ms.");
			while (result.hasNext())
			{
				Record row = result.next();
				ttr.put(row.get("d.id").toString(), Double.parseDouble(row.get("ttr").toString()));
			}
		} catch (Exception e)
		{
			System.out.println("getTTR(ArrayList) query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("getTTR(ArrayList) took: " + (endTime - startTime) + " ms.");
		}
		return ttr;
	}

	/**
	 * Optimized helper for {@link #calculateTTRForCollectionOfDocuments calculateTTRForCollectionOfDocuments}, and should be for {@link #calculateTTRForAllDocuments calculateTTRForAllDocuments}.
	 * Checks and returns saved TTR values from nodes, which is much faster.
	 *
	 * @param documentIds the specified documents id's.
	 * @return a Map(String lemma, Double saved TTR-value)
	 */
	private Map<String, Double> getTTR_opt(Collection<String> documentIds)
	{
		long startTime = System.currentTimeMillis();
		Map<String, Double> ttr = new HashMap<>();
		try
		{
			long startTimeQ = System.currentTimeMillis();
			StatementResult result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT) WHERE d.id IN [" + documentIds.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + "] RETURN d.id, d.ttr AS ttr;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("getTTR_opt query took: " + (endTimeQ - startTimeQ) + " ms.");
			while (result.hasNext())
			{
				Record row = result.next();
				if (row.get("ttr") != null && row.get("d.id") != null)
					ttr.put(row.get("d.id").toString().replaceAll("\"", ""), Double.parseDouble(row.get("ttr").toString()));
			}
		} catch (Exception e)
		{
			System.out.println("getTTR_opt query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("getTTR_opt(ArrayList) took: " + (endTime - startTime) + " ms.");
		}
		return ttr;
	}

	/**
	 * Count all elements of the specified <i>type</i>.
	 *
	 * @param type Instance of Const.TYPE, namely TOKEN, LEMMA, POS,
	 *             DOCUMENT, SENTENCE or PARAGRAPH.
	 * @return the count of <i>type</i> or 0 if there are none or the query fails.
	 */
	public int countElementsOfType(Const.TYPE type)
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (n:" + type.toString() + ") RETURN count(n) AS count;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("count(" + type.toString() + ") query took: " + (endTimeQ - startTimeQ) + " ms.");
		} catch (Exception e)
		{
			System.out.println("count(" + type.toString() + ") query failed!");
			e.printStackTrace();
			return 0;
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("count(" + type.toString() + ") took: " + (endTime - startTime) + " ms.");
		}
		return Integer.parseInt(result.next().get("count").toString());
	}

	/**
	 * Counts all elements of <i>type</i> in the specified document.
	 *
	 * @param documentId The id of the document from which elements are to be counted.
	 * @param type       Instance of Const.TYPE, namely TOKEN, LEMMA, POS,
	 *                   SENTENCE or PARAGRAPH.
	 * @return the count of <i>type</i> in the specified document as an {@link Integer} or 0 if there are none or the query fails.
	 */
	public int countElementsInDocumentOfType(String documentId, Const.TYPE type)
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			switch (type)
			{
				case LEMMA:
					result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT  {id:'" + documentId + "'})--(l:LEMMA) RETURN count(l) AS count;");
					break;
				case TOKEN:
					result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT  {id:'" + documentId + "'})--(t:TOKEN) RETURN count(t) AS count;");
					break;
				case POS:
					result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT  {id:'" + documentId + "'})--(:TOKEN)--(p:POS) RETURN count(DISTINCT p.value) AS count;");
					break;
				case SENTENCE:
					result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT  {id:'" + documentId + "'})--(s:SENTENCE) RETURN count(DISTINCT s) AS count;");
					break;
				case PARAGRAPH:
					result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT  {id:'" + documentId + "'})--(p:PARAGRAPH) RETURN count(DISTINCT p) AS count;");
					break;
				default:
					throw (new IllegalArgumentException("Invalid argument provided for count(String, Const.TYPE)! Argument was: " + type.toString()));
			}

			long endTimeQ = System.currentTimeMillis();
			System.out.println("count(" + type.toString() + ") query took: " + (endTimeQ - startTimeQ) + " ms.");
		} catch (Exception e)
		{
			System.out.println("count(" + type.toString() + ") query failed!");
			e.printStackTrace();
			return 0;
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("count(" + type.toString() + ") took: " + (endTime - startTime) + " ms.");
		}
		return Integer.parseInt(result.next().get("count").toString());
	}

	/**
	 * Counts all elements of <i>type</i> across all documents with given <i>value</i>.
	 *
	 * @param type  Instance of Const.TYPE, namely TOKEN, LEMMA or POS.
	 * @param value String, value of the element.
	 * @return the count of <i>type</i> with the specified value as an {@link Integer} or 0 if there are none or the query fails.
	 * @throws IllegalArgumentException when the <i>type</i>
	 *                                  given does not match TOKEN, LEMMA or POS.
	 */
	public int countElementsOfTypeWithValue(Const.TYPE type, String value)
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			switch (type)
			{
				case TOKEN:
					result = this.driver.runSimpleQuery("MATCH (t:TOKEN {value:'" + value + "'}) RETURN count(t) AS count;");
					break;
				case LEMMA:
					result = this.driver.runSimpleQuery("MATCH (t:TOKEN)--(l:LEMMA {value:'" + value + "'}) RETURN count(DISTINCT t) AS count;");
					break;
				case POS:
					result = this.driver.runSimpleQuery("MATCH (t:TOKEN)--(p:POS {value:'" + value + "'}) RETURN count(DISTINCT t) AS count;");
					break;
				default:
					throw (new IllegalArgumentException("Invalid argument provided for count(Const.TYPE, String)! Arguments were: " + type.toString() + ", " + value + ".\nValid arguments are: Const.TYPE.TOKEN|LEMMA|POS, String"));
			}
			long endTimeQ = System.currentTimeMillis();
			System.out.println("count(" + type.toString() + ", " + value + ") query took: " + (endTimeQ - startTimeQ) + " ms.");
		} catch (Exception e)
		{
			System.out.println("count(" + type.toString() + ", " + value + ") query failed!");
			e.printStackTrace();
			return 0;
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("count(" + type.toString() + ", " + value + ") took: " + (endTime - startTime) + " ms.");
		}
		return Integer.parseInt(result.next().get("count").toString());
	}

	/*
	 * Counts all nodes of <i>type</i> with <i>value</i> in the document with the id <i>docId</i>.
	 * @param docId the document of the counting scope.
	 * @param type type of the nodes to be counted.
	 * @param value the <i>value</i> to match to the nodes property 'value'.
	 * @return int the number of nodes matching the parameters.
	 * @throws InvalidParameterException when the <i>type</i> given does not match TOKEN, LEMMA or POS.
	 */
	public int countElementsInDocumentOfTypeWithValue(String docId, Const.TYPE type, String value) throws InvalidParameterException
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			switch (type)
			{
				case TOKEN:
					result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT{id: '" + docId + "'})--(t:TOKEN {value:'" + value + "'}) RETURN count(DISTINCT t) AS count;");
					break;
				case LEMMA:
					result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT{id: '" + docId + "'})--(t:TOKEN)--(l:LEMMA {value:'" + value + "'}) RETURN count(DISTINCT t) AS count;");
					break;
				case POS:
					result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT{id: '" + docId + "'})--(t:TOKEN)--(p:POS {value:'" + value + "'}) RETURN count(DISTINCT t) AS count;");
					break;
				default:
					throw (new InvalidParameterException("Invalid parameter provided for count(String, Const.TYPE, String)! Arguments were: " + type.toString() + ".\nValid arguments are: Const.TYPE.TOKEN|LEMMA|POS"));
			}
			long endTimeQ = System.currentTimeMillis();
			System.out.println("count(" + type.toString() + ", " + value + ") query took: " + (endTimeQ - startTimeQ) + " ms.");
		} catch (Exception e)
		{
			System.out.println("count(" + type.toString() + ", " + value + ") query failed!");
			e.printStackTrace();
			return 0;
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("count(" + type.toString() + ", " + value + ") took: " + (endTime - startTime) + " ms.");
		}
		return Integer.parseInt(result.next().get("count").toString());
	}

	/**
	 * Computes the term-frequency without norming for all lemmata in the specified document.
	 * <p>When this query is first run for a document, the count of all lemmata is saved as a property
	 * in their {@link org.hucompute.services.uima.database.neo4j.data.Const.RelationType inDocument} relationship.
	 *
	 * @param docId the specified document's id.
	 * @return a HashMap(String lemma, Double raw-term-frequency).
	 */
	public TreeMap<String, Double> rawTermFrequencies(String docId)
	{
		long startTime = System.currentTimeMillis();
		TreeMap<String, Double> rtf = new TreeMap<>();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT {id:'" + docId + "'})-[r:inDocument]-(l:LEMMA) WHERE exists(r.count) RETURN l.value AS lemma, r.count AS count");
			if (result == null || !result.hasNext())
			{
				result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT {id:'" + docId + "'})--(:TOKEN)--(l:LEMMA)-[r:inDocument]-(d:DOCUMENT {id:'" + docId + "'}) WITH l, r, count(l.value) AS count SET r.count = count RETURN l.value AS lemma, count;");
			}

			long endTimeQ = System.currentTimeMillis();
			System.out.println("rawTermFrequencies(" + docId + ") query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				rtf.put(row.get("lemma").toString().replaceAll("\"", ""), Double.parseDouble(row.get("count").toString()));
			}
		} catch (Exception e)
		{
			System.out.println("rawTermFrequencies(" + docId + ") query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("rawTermFrequencies(" + docId + ") took: " + (endTime - startTime) + " ms.");
		}
		return rtf;
	}

	public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(String lemma, String docId)
	{
		TreeMap<String, Double> rtf = rawTermFrequencies(docId);
		return (0.5 + 0.5 * ((double) rtf.getOrDefault(lemma, 0.0) / (double) Collections.max(rtf.values())));
	}

	public double calculateTermFrequencyWithLogNermForLemmaInDocument(String lemma, String docId)
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		double tf = 0;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT {id:'" + docId + "'})-[r:inDocument]-(l:LEMMA {value: '" + lemma + "'}) WHERE exists(r.count) RETURN r.count AS count");
			if (result == null || !result.hasNext())
			{
				result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT {id:'" + docId + "'})--(:TOKEN)--(l:LEMMA {value: '" + lemma + "'}) RETURN count(l) AS count;");
			}
			long endTimeQ = System.currentTimeMillis();
			System.out.println("termFrequency_logNorm(" + docId + ", " + lemma + ") query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				tf = Double.parseDouble(row.get("count").toString());
			}
		} catch (Exception e)
		{
			System.out.println("termFrequency_logNorm(" + docId + ", " + lemma + ") query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("termFrequency_logNorm(" + docId + ", " + lemma + ") took: " + (endTime - startTime) + " ms.");
		}
		return 1 + Math.log(tf);
	}

	public Map<String, Double> calculateTermFrequenciesForLemmataInDocument(String docId)
	{
		TreeMap<String, Double> rtf = rawTermFrequencies(docId);
		HashMap<String, Double> tf = new HashMap<>();

		try
		{
			double max = Collections.max(rtf.values());
			rtf.entrySet().stream().forEach(e -> {
				tf.put(e.getKey(), 0.5 + 0.5 * (e.getValue() / max));
			});
		} catch (NoSuchElementException e)
		{
			System.out.println(e.toString());
			System.out.println("Document " + docId + " does not have lemmata.");
		}
		return tf;
	}

	/**
	 * @param lemma
	 * @return
	 */
	@Deprecated
	public double documentsContaining(String lemma)
	{
		long startTime = System.currentTimeMillis();
		double dc = 0.0;
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)--(:TOKEN)--(l:LEMMA {value: '" + lemma.replaceAll("\"\'", "") + "'}) RETURN count(d) AS count");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("documentsContaining(" + lemma + ") query took: " + (endTimeQ - startTimeQ) + " ms.");

			Record row = result.next();
			dc = Double.parseDouble(row.get("count").toString());
		} catch (Exception e)
		{
			System.out.println("documentsContaining(" + lemma + ") query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("documentsContaining(" + lemma + ") took: " + (endTime - startTime) + " ms.");
		}
		return dc;
	}

	public int countDocumentsContainingLemma(String lemma)
	{
		long startTime = System.currentTimeMillis();
		int dc = 0;
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)-[:inDocument]-(l:LEMMA {value: '" + lemma.replaceAll("\"\'", "") + "'}) RETURN count(d) AS count");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("documentsContaining_opt(" + lemma + ") query took: " + (endTimeQ - startTimeQ) + " ms.");

			Record row = result.next();
			dc = Integer.parseInt(row.get("count").toString());
		} catch (Exception e)
		{
			System.out.println("documentsContaining_opt(" + lemma + ") query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("documentsContaining_opt(" + lemma + ") took: " + (endTime - startTime) + " ms.");
		}
		return dc;
	}

	@Deprecated
	public double inverseDocumentFrequency(String lemma)
	{
		return Math.log((countElementsOfType(Const.TYPE.DOCUMENT) / documentsContaining(lemma)));
	}

	public double calculateInverseDocumentFrequency(String lemma)
	{
		return Math.log((countElementsOfType(Const.TYPE.DOCUMENT) / (double) countDocumentsContainingLemma(lemma)));
	}

	@Deprecated
	public HashMap<String, Double> inverseDocumentFrequencies(String docId)
	{
		double docCount = (double) countElementsOfType(Const.TYPE.DOCUMENT);
		HashMap<String, Double> idf = new HashMap<>();
		HashSet<String> lemmata = getLemmata(docId);
		lemmata.stream().forEach(e -> {
			try
			{
				idf.put(e, Math.log(docCount / documentsContaining(e)));
			} catch (Exception e1)
			{
				e1.printStackTrace();
			}
		});
		return idf;
	}

	public Map<String, Double> calculateInverseDocumentFrequenciesForLemmataInDocument(String docId)
	{
		double docCount = (double) countElementsOfType(Const.TYPE.DOCUMENT);
		HashMap<String, Double> idf = new HashMap<>();
		Set<String> lemmata = getLemmataForDocument(docId);
		HashMap<String, Double> dC = documentsContaining_all();
		lemmata.stream().forEach(e -> {
			idf.put(e.replaceAll("\"", ""), Math.log(docCount / dC.getOrDefault(e, 1.0)));
		});
		return idf;
	}

	/**
	 * Counts how often any lemma is contained in all documents.
	 *
	 * @return a HashMap(String lemma, Double lemma count)
	 */
	private HashMap<String, Double> documentsContaining_all()
	{
		long startTime = System.currentTimeMillis();
		HashMap<String, Double> dC = new HashMap<>();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (:DOCUMENT)-[:inDocument]-(l:LEMMA) RETURN l.value AS LEMMA, count(l) AS count;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("documentsContaining_all() query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				dC.put(row.get("lemma").toString(), Double.parseDouble(row.get("count").toString()));
			}
		} catch (Exception e)
		{
			System.out.println("documentsContaining_all() query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("documentsContaining_all() took: " + (endTime - startTime) + " ms.");
		}
		return dC;
	}

	@Deprecated
	public HashSet<String> getLemmata(String docId)
	{
		long startTime = System.currentTimeMillis();
		HashSet<String> lemmata = new HashSet<>();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT {id:'" + docId + "'})--(:TOKEN)--(l:LEMMA) RETURN l.value AS lemma;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("getLemmata(" + docId + ") query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				lemmata.add(row.get("lemma").toString());
			}
		} catch (Exception e)
		{
			System.out.println("getLemmata(" + docId + ") query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("getLemmata(" + docId + ") took: " + (endTime - startTime) + " ms.");
		}
		return lemmata;
	}

	public Set<String> getLemmataForDocument(String docId)
	{
		long startTime = System.currentTimeMillis();
		HashSet<String> lemmata = new HashSet<>();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT {id:'" + docId + "'})-[:inDocument]-(l:LEMMA) RETURN l.value AS lemma;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("getLemmata_opt(" + docId + ") query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				lemmata.add(row.get("lemma").toString());
			}
		} catch (Exception e)
		{
			System.out.println("getLemmata_opt(" + docId + ") query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("getLemmata_opt(" + docId + ") took: " + (endTime - startTime) + " ms.");
		}
		return lemmata;
	}

	@Override
	public void storeJCasDocument(JCas document)
	{
		// TODO: implement
	}

	@Override
	public void storeSentence(Sentence sentence, JCas document, Paragraph paragraph, Sentence previousSentence)
	{

	}

	@Override
	public void storeSentence(Sentence sentence, JCas document, Paragraph paragraph)
	{

	}

	@Override
	public void storeToken(Token token, JCas document, Paragraph paragraph, Sentence sentence, Token previousToken)
	{

	}

	@Override
	public void storeToken(Token token, JCas document, Paragraph paragraph, Sentence sentence)
	{

	}

	@Override
	public void storeJCasDocuments(Iterable<JCas> documents)
	{
		// TODO: implement
	}

	@Override
	public void getDocumentsAsJCas()
	{
		// TODO: implement
	}

	public double calculateTFIDFForLemmaInDocument(String lemma, String docId)
	{
		long startTime = System.currentTimeMillis();
		double tfidf = calculateTermFrequencyWithLogNermForLemmaInDocument(lemma, docId) * calculateInverseDocumentFrequency(lemma);
		long endTime = System.currentTimeMillis();
		System.out.println("tfidf(" + docId + ", " + lemma + ") took: " + (endTime - startTime) + " ms.");
		return tfidf;
	}

	public Map<String, Double> calculateTFIDFForLemmataInDocument(String docId)
	{
		long startTime = System.currentTimeMillis();
		Map<String, Double> tf = calculateTermFrequenciesForLemmataInDocument(docId);
		Map<String, Double> idf = calculateInverseDocumentFrequenciesForLemmataInDocument(docId);
		HashMap<String, Double> tfidf = new HashMap<>();
		tf.entrySet().stream().forEach(e -> tfidf.put(e.getKey(), e.getValue() * idf.get(e.getKey())));
		long endTime = System.currentTimeMillis();
		System.out.println("tfidf_all(" + docId + ") took: " + (endTime - startTime) + " ms.");
		return tfidf;
	}

	public Map<String, Map<String, Double>> calculateTFIDFForLemmataInAllDocuments()
	{
		long startTime = System.currentTimeMillis();
		HashMap<String, Map<String, Double>> tfidfs = new HashMap<>();
		getDocIds().forEach(f -> {
			try
			{
				tfidfs.put(f, calculateTFIDFForLemmataInDocument(f));
			} catch (Exception e1)
			{
				e1.printStackTrace();
			}
		});
		long endTime = System.currentTimeMillis();
		System.out.println("calculateTFIDFForLemmataInAllDocuments() took: " + (endTime - startTime) + " ms.");
		return tfidfs;
	}

	/**
	 * Get all document ids.
	 *
	 * @return an ArrayList&lt;documentId&gt;
	 */
	private ArrayList<String> getDocIds()
	{
		long startTime = System.currentTimeMillis();
		ArrayList<String> docIds = new ArrayList<>();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT) RETURN d.id as id;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("getDocIds() query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				docIds.add(row.get("id").toString().replaceAll("\"", ""));
			}
		} catch (Exception e)
		{
			System.out.println("getDocIds() query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("getDocIds() took: " + (endTime - startTime) + " ms.");
		}
		return docIds;
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId)
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		ArrayList<String> biGrams = new ArrayList<>();
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT {id: '" + documentId.replaceAll("\"", "") + "'})--(t1:TOKEN)<-[:successorT]-(t2:TOKEN) RETURN t1.value, t2.value ORDER BY t1.value, t2.value;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("biGram(" + documentId + ") query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				biGrams.add(row.get("t1.value").toString().replaceAll("\"", "") + "-" + row.get("t2.value").toString().replaceAll("\"", ""));
			}
		} catch (Exception e)
		{
			System.out.println("biGram(" + documentId + ") query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("biGram(" + documentId + ") took: " + (endTime - startTime) + " ms.");
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		ArrayList<String> biGrams = new ArrayList<>();
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)--(t1:TOKEN)<-[:successorT]-(t2:TOKEN) RETURN d.id, t1.value, t2.value ORDER BY d.id, t1.value, t2.value;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("biGram) query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				biGrams.add(row.get("d.id").toString().replaceAll("\"", "") + ":" + row.get("t1.value").toString().replaceAll("\"", "") + "-" + row.get("t2.value").toString().replaceAll("\"", ""));
			}
		} catch (Exception e)
		{
			System.out.println("biGram() query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("biGram() took: " + (endTime - startTime) + " ms.");
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(Collection<String> documentIds)
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		ArrayList<String> biGrams = new ArrayList<>();
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)--(t1:TOKEN)<-[:successorT]-(t2:TOKEN) WHERE d.id IN [" + documentIds.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + "] RETURN d.id, t1.value, t2.value ORDER BY d.id, t1.value, t2.value;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("biGram) query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				biGrams.add(row.get("d.id").toString().replaceAll("\"", "") + ":" + row.get("t1.value").toString().replaceAll("\"", "") + "-" + row.get("t2.value").toString().replaceAll("\"", ""));
			}
		} catch (Exception e)
		{
			System.out.println("biGram() query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("biGram() took: " + (endTime - startTime) + " ms.");
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		ArrayList<String> triGrams = new ArrayList<>();
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT {id: '" + documentId.replaceAll("\"", "") + "'})--(t1:TOKEN)<-[:successorT]-(t2:TOKEN)<-[:successorT]-(t3:TOKEN) RETURN t1.value, t2.value, t3.value ORDER BY t1.value, t2.value, t3.value;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("biGram(" + documentId + ") query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				triGrams.add(row.get("t1.value").toString().replaceAll("\"", "") + "-" + row.get("t2.value").toString().replaceAll("\"", "") + "-" + row.get("t3.value").toString().replaceAll("\"", ""));
			}
		} catch (Exception e)
		{
			System.out.println("biGram(" + documentId + ") query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("biGram(" + documentId + ") took: " + (endTime - startTime) + " ms.");
		}
		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		ArrayList<String> triGrams = new ArrayList<>();
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)--(t1:TOKEN)<-[:successorT]-(t2:TOKEN)<-[:successorT]-(t3:TOKEN) RETURN d.id, t1.value, t2.value, t3.value ORDER BY d.id, t1.value, t2.value, t3.value;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("biGram) query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				triGrams.add(row.get("d.id").toString().replaceAll("\"", "") + ":" + row.get("t1.value").toString().replaceAll("\"", "") + "-" + row.get("t2.value").toString().replaceAll("\"", "") + "-" + row.get("t3.value").toString().replaceAll("\"", ""));
			}
		} catch (Exception e)
		{
			System.out.println("biGram() query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("biGram() took: " + (endTime - startTime) + " ms.");
		}
		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(Collection<String> documentIds)
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		ArrayList<String> triGrams = new ArrayList<>();
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)--(t1:TOKEN)<-[:successorT]-(t2:TOKEN)<-[:successorT]-(t3:TOKEN) WHERE d.id IN [" + documentIds.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + "] RETURN d.id, t1.value, t2.value, t3.value ORDER BY d.id, t1.value, t2.value, t3.value;");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("biGram) query took: " + (endTimeQ - startTimeQ) + " ms.");

			while (result.hasNext())
			{
				Record row = result.next();
				triGrams.add(row.get("d.id").toString().replaceAll("\"", "") + ":" + row.get("t1.value").toString().replaceAll("\"", "") + "-" + row.get("t2.value").toString().replaceAll("\"", "") + "-" + row.get("t3.value").toString().replaceAll("\"", ""));
			}
		} catch (Exception e)
		{
			System.out.println("biGram() query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("biGram() took: " + (endTime - startTime) + " ms.");
		}
		return triGrams;
	}

	@Deprecated
	public StatementResult connectDocumentLemma()
	{
		long startTime = System.currentTimeMillis();
		StatementResult result = null;
		try
		{
			long startTimeQ = System.currentTimeMillis();
			result = this.driver.runSimpleQuery("MATCH (d:DOCUMENT)--(:TOKEN)--(l:LEMMA) CREATE UNIQUE (d)<-[:inDocument]-(l);");
			long endTimeQ = System.currentTimeMillis();
			System.out.println("connectDocumentLemma() query took: " + (endTimeQ - startTimeQ) + " ms.");
		} catch (Exception e)
		{
			System.out.println("connectDocumentLemma() query failed!");
			e.printStackTrace();
		} finally
		{
			long endTime = System.currentTimeMillis();
			System.out.println("connectDocumentLemma() took: " + (endTime - startTime) + " ms.");
		}
		return result;
	}

	/**
	 * Get the db_link value from the conf.conf file.
	 *
	 * @param confFile the path to the conf.conf file, presumed to be 'src/main/resources/neo4j/conf.conf' by default.
	 * @return a string.
	 */
	public static String getDbLink(String confFile)
	{
		if (confFile == null)
			confFile = "src/main/resources/neo4j/conf.conf";
		Map<String, String> conf = new HashMap<>();
		Properties lProperties = new Properties();
		try
		{
			lProperties.load(new FileInputStream(new File(confFile)));

		} catch (IOException e)
		{
			e.printStackTrace();
		}
		for (String lString : lProperties.stringPropertyNames())
		{
			conf.put(lString, (String) lProperties.get(lString));
		}
		return conf.get("db_link");
	}

	/**
	 * Write the <i>fileContent</i> to a new file, using a BufferedWriter.
	 *
	 * @param outFileName the file name.
	 * @param fileContent a String with the files contents.
	 */
	public static void writeToFile(String outFileName, String fileContent)
	{
		Charset charset = Charset.forName("US-ASCII");
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outFileName), charset))
		{
			writer.write(fileContent, 0, fileContent.length());
			System.out.println(outFileName + " created.");
		} catch (IOException x)
		{
			System.err.format("IOException: %s%n", x);
		}
	}

	/**
	 * Legacy function, do not use.
	 */
	@Deprecated
	public static void wellconnect()
	{
		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
		{
			//Create a relationship between all documents and the lemmas of their tokens
			Neo4jDriver.executeSimpleQuery("MATCH (d:DOCUMENT)--(:TOKEN)--(l:LEMMA) CREATE UNIQUE (d)<-[:inDocument]-(l);");
			tx.success();
		}
		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
		{
			//Create a relationship between all paragraphs and the sentences, tokens contained in them
			Neo4jDriver.executeSimpleQuery("MATCH (p:PARAGRAPH)--(d:DOCUMENT)--(t:TOKEN) WHERE p.begin<=t.begin AND p.end>=t.begin CREATE UNIQUE (p)<-[:inParagraphT]-(t);");
			tx.success();
		}
		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
		{
			Neo4jDriver.executeSimpleQuery("MATCH (p:PARAGRAPH)--(d:DOCUMENT)--(s:SENTENCE) WHERE p.begin<=s.begin AND p.end>=s.begin CREATE UNIQUE (p)<-[:inParagraphS]-(s);");
			tx.success();
		}
		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
		{
			//Create a relationship between all sentences and the token contained in them
			Neo4jDriver.executeSimpleQuery("MATCH (t:TOKEN)--(d:DOCUMENT)--(s:SENTENCE) WHERE s.begin<=t.begin AND s.end>=t.begin CREATE UNIQUE (s)<-[:inSentence]-(t);");
			tx.success();
		}

	}

}
