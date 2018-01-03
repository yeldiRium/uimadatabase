package org.hucompute.services.uima.database;

import org.hucompute.services.uima.database.neo4j.data.Const;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * The interface for all relevant, server queries. See
 * {@link AbstractQueryHandler} for an implementation.
 *
 * @author Manuel Stoeckel
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 * Created on 26.09.2017
 */
interface QueryHandlerInterface extends Callable<JSONObject>
{

	//--------------------------------------------------------------------------
	// Class functions
	//--------------------------------------------------------------------------

	/**
	 * Intended to interpret a simple query string and execute methods
	 * accordingly.
	 * If this can be efficiently used is questionable.
	 *
	 * @param query An ArrayDeque of Strings containing commands and parameters.
	 * @return The result of the query formatted as a JSONObject.
	 * @throws UnsupportedOperationException if the query contains an unsup-
	 *                                       ported command.
	 * @throws NoSuchElementException        if the query asks for an element
	 *                                       which doesn't exist.
	 * @throws IllegalArgumentException      if the query is malformed.
	 */
	JSONObject interpret(ArrayDeque<String> query)
			throws UnsupportedOperationException, NoSuchElementException,
			IllegalArgumentException;

	//--------------------------------------------------------------------------
	// Raw Querying
	//
	// Meaning everything that has no analysis directly attached. Counting ele-
	// ments is not interpreted as analysis.
	// This includes read and write operations.
	//--------------------------------------------------------------------------

	/**
	 * A set of all lemmata in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashSet(String lemma).
	 */
	HashSet<String> get_Lemmata(String documentId);

	/**
	 * Count all elements of the specified <i>type</i>.
	 *
	 * @param type Instance of Const.TYPE, namely TOKEN, LEMMA, POS,
	 *             DOCUMENT, SENTENCE or PARAGRAPH.
	 * @return An integer.
	 */
	int count_type(Const.TYPE type);

	/**
	 * Counts all elements of <i>type</i> in the specified document.
	 *
	 * @param documentId The id of the document from which elements are to be
	 *                   counted.
	 * @param type       Instance of Const.TYPE, namely TOKEN, LEMMA, POS,
	 *                   DOCUMENT, SENTENCE or PARAGRAPH.
	 * @return An integer.
	 */
	int count_type_in_document(String documentId, Const.TYPE type);

	/**
	 * Counts all elements of <i>type</i> across all documents with given
	 * <i>value</i>.
	 *
	 * @param type  Instance of Const.TYPE, namely TOKEN, LEMMA or POS.
	 * @param value String, value of the element.
	 * @return An integer.
	 * @throws IllegalArgumentException when the <i>type</i>
	 *                                  given does not match TOKEN, LEMMA or
	 *                                  POS.
	 */
	int count_type_with_value(Const.TYPE type, String value)
			throws IllegalArgumentException;

	/**
	 * Counts all elements of <i>type</i> within one specified document with gi-
	 * ven <i>value</i>.
	 *
	 * @param documentId The id of the document from which elements are to be
	 *                   counted.
	 * @param type       Instance of Const.TYPE, namely TOKEN, LEMMA or POS.
	 * @param value      String, value of the element.
	 * @return An integer.
	 * @throws IllegalArgumentException when the <i>type</i>
	 *                                  given does not match TOKEN, LEMMA or
	 *                                  POS.
	 */
	int count_type_with_value_in_document(
			String documentId, Const.TYPE type, String value
	) throws IllegalArgumentException;


	//--------------------------------------------------------------------------
	// TTR
	// TTR = total number of Types / total number of Tokens
	// The TTR is indicative for the size of the vocabulary used in a text,
	// since it relates the amount of <i>words</i> with the amount of different
	// <i>word stems<i>.
	//--------------------------------------------------------------------------

	/**
	 * Get TTR for all documents.
	 *
	 * @return Map(String DocumentId, Double TTR - value)
	 */
	Map<String, Double> TTR_all();

	/**
	 * Get TTR for the specified document.
	 *
	 * @param documentId The id of the document to get the TTR from.
	 * @return Map(String DocumentId, Double TTR - value)
	 */
	Map<String, Double> TTR_one(String documentId);

	/**
	 * Get TTR for all specified documents.
	 *
	 * @param documentIds The id's of the document to get the TTR from.
	 * @return Map(String DocumentId, Double TTR - value)
	 */
	Map<String, Double> TTR_collection(Collection<String> documentIds);


	//--------------------------------------------------------------------------
	// TF-IDF
	// term frequency - inverse document frequency
	// "is a numerical statistic that is intended to reflect how important a
	// word is to a document in a collection or corpus"
	//  - https://en.wikipedia.org/wiki/Tf-idf
	//
	// Term Frequency: How often a term occurs in a given document.
	// Inverse Document Frequency: How rare a term is in a corpus of documents.
	//--------------------------------------------------------------------------

	/**
	 * The term-frequency normed with 0.5 (a double).
	 *
	 * @param documentId the specified document's id.
	 * @param lemma      the specified lemma.
	 * @return a double.
	 */
	double get_termFrequency_doubleNorm(String documentId, String lemma);

	/**
	 * The term-frequency normed with the natural logarithm.
	 *
	 * @param documentId the specified document's id.
	 * @param lemma      the specified lemma.
	 * @return a double.
	 */
	double get_termFrequency_logNorm(String documentId, String lemma);

	/**
	 * Compute and return term-frequencies for all lemmata in the specified
	 * document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double term-frequency).
	 */
	HashMap<String, Double> get_termFrequencies(String documentId);

	/**
	 * The amount of documents that contain at least one instance of
	 * <i>lemma</i>.
	 *
	 * @param lemma the specified lemma.
	 * @return a double (cast from int).
	 */
	double get_documentsContaining(String lemma);

	/**
	 * The natural logarithm from the division of the count of documents and the
	 * documents containing <i>lemma</i>.
	 *
	 * @param lemma the specified lemma.
	 * @return a double.
	 */
	double get_inverseDocumentFrequency(String lemma);

	/**
	 * Inverse-document-frequencies for all lemmata in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double inverse-document-frequency).
	 */
	HashMap<String, Double> get_inverseDocumentFrequencies(String documentId);

	/**
	 * The TF-IDF for <i>lemma</i> in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @param lemma      the specified lemma.
	 * @return a double.
	 */
	double get_tfidf(String documentId, String lemma);

	/**
	 * The TF-IDF for <b>all lemmata</b> in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double tfidf).
	 */
	HashMap<String, Double> get_tfidf_all(String documentId);

	/**
	 * The TF-IDF for <b>all lemmata</b> in <i>all</i> documents.
	 * <b>Warning:</b> May produce heavy server load.
	 *
	 * @return a map from document id to a map from lemma to tfidf.
	 */
	HashMap<String, HashMap<String, Double>> get_tfidf_all_all();


	//--------------------------------------------------------------------------
	// Utility
	//
	// Bi-Grams are all pairs of neighbouring tokens in a string.
	// Tri-Grams are thus all triplets of neighbouring tokens in a string.
	//--------------------------------------------------------------------------

	/**
	 * <p>A List of all token-bi-grams in the specified document.</p>
	 * The bi-gram is represented by a string, which is the product
	 * of the concatenation of both parts of the bi-gramm, spepareted by a '-'.
	 *
	 * @param documentId the specified document's id.
	 * @return an ArrayList(String bi-gram)
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	ArrayList<String> get_bi_grams(String documentId)
			throws UnsupportedOperationException;

	/**
	 * A list of all token-bi-grams from all documents.
	 * <b>Warning:</b> May produce heavy server load.
	 * See {@link #get_bi_grams get_bi_grams} for details.
	 *
	 * @return an ArrayList(String bi-gram).
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	ArrayList<String> get_bi_grams_all() throws UnsupportedOperationException;

	/**
	 * A list of all token-bi-grams from all documents specified in the collec-
	 * tion.
	 * See {@link #get_bi_grams get_bi_grams} for details.
	 *
	 * @param documentIds the specified document's id.
	 * @return an ArrayList(String bi-gram).
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	ArrayList<String> get_bi_grams_collection(Collection<String> documentIds)
			throws UnsupportedOperationException;

	/**
	 * <p>A List of all token-tri-grams in the specified document.</p>
	 * The tri-gram is represented by a string, which is the product
	 * of the concatenation of all parts of the tri-gramm, spepareted by a '-'.
	 *
	 * @param documentId the specified document's id.
	 * @return an ArrayList(String tri-gram)
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	ArrayList<String> get_tri_grams(String documentId)
			throws UnsupportedOperationException;

	/**
	 * A list of all token-tri-grams from all documents. <b>Warning:</b> May
	 * produce heavy server load.
	 * See {@link #get_tri_grams get_tri_grams} for details.
	 *
	 * @return an ArrayList(String tri-gram).
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	ArrayList<String> get_tri_grams_all() throws UnsupportedOperationException;

	/**
	 * A list of all token-tri-grams from all documents specified in the
	 * collection.
	 * See {@link #get_tri_grams get_tri_grams} for details.
	 *
	 * @param documentIds the specified documents id's.
	 * @return an ArrayList(String tri-gram).
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	ArrayList<String> get_tri_grams_collection(Collection<String> documentIds)
			throws UnsupportedOperationException;
}
