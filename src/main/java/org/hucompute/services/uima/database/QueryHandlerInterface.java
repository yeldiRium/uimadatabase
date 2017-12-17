package org.hucompute.services.uima.database;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import org.hucompute.services.uima.database.neo4j.data.Const;
import org.json.JSONObject;

/**
 * The interface for all relevant, server queries. See {@link AbstractQueryHandler} for an implementation.
 * @author Manuel Stoeckel
 * Created on 26.09.2017
 */
interface QueryHandlerInterface extends Callable<JSONObject> {
	
	//---------------------------------------------------------------------------
	// Class functions
	//---------------------------------------------------------------------------
	
	JSONObject interpret(ArrayDeque<String> query)
			throws UnsupportedOperationException, NoSuchElementException, IllegalArgumentException;
	
	
	//---------------------------------------------------------------------------
	// TTR
	//---------------------------------------------------------------------------
	
	/**
	 * Get TTR for all documents.
	 * @return Map(String DocumentId, Double TTR-value)  
	 */
	Map<String, Double> TTR_all();
	
	/**
	 * Get TTR for the specified document.
	 * @param documentId The id of the document to get the TTR from.
	 * @return Map(String DocumentId, Double TTR-value)
	 */
	Map<String, Double> TTR_one(String documentId);

	/**
	 * Get TTR for all specified documents.
	 * @param documentIds  The id's of the document to get the TTR from.
	 * @return Map(String DocumentId, Double TTR-value)
	 */
	Map<String, Double> TTR_collection(Collection<String> documentIds);
	
	
	//---------------------------------------------------------------------------
	// Count
	//---------------------------------------------------------------------------
	
	/**
	 * Count all elements of the specified <i>type</i>.
	 * @param type Instance of Const.TYPE, namely TOKEN, LEMMA, POS, 
	 * DOCUMENT, SENTENCE or PARAGRAPH.
	 * @return An integer.
	 */
	int count_type (Const.TYPE type);
	
	/**
	 * Counts all elements of <i>type</i> in the specified document.
	 * @param documentId The id of the document from which elements are to be counted.
	 * @param type Instance of Const.TYPE, namely TOKEN, LEMMA, POS,
	 * DOCUMENT, SENTENCE or PARAGRAPH.
	 * @return An integer.
	 */
	int count_type_in_document (String documentId, Const.TYPE type);
	
	/**
	 * Counts all elements of <i>type</i> across all documents with given <i>value</i>. 
	 * @param type Instance of Const.TYPE, namely TOKEN, LEMMA or POS.
	 * @param value String, value of the element.
	 * @return An integer.
	 * @throws IllegalArgumentException when the <i>type</i>
	 * given does not match TOKEN, LEMMA or POS.
	 */
	int count_type_with_value (Const.TYPE type, String value) throws IllegalArgumentException;
	
	/**
	 * Counts all elements of <i>type</i> within one specified document with given <i>value</i>. 
	 * @param documentId The id of the document from which elements are to be counted.
	 * @param type Instance of Const.TYPE, namely TOKEN, LEMMA or POS.
	 * @param value String, value of the element.
	 * @return An integer.
	 * @throws IllegalArgumentException when the <i>type</i>
	 * given does not match TOKEN, LEMMA or POS.
	 */
	int count_type_with_value_in_document (String documentId, Const.TYPE type, String value)
			throws IllegalArgumentException;
		
	
	//---------------------------------------------------------------------------
	// TF-IDF
	//---------------------------------------------------------------------------
	
	/**
	 * The term-frequency normed with 0.5 (a double).
	 * @param documentId the specified document's id.
	 * @param lemma the specified lemma.
	 * @return a double.
	 */
	double get_termFrequency_doubleNorm (String documentId, String lemma);
	
	/**
	 * The term-frequency normed with the natural logarithm.
	 * @param documentId the specified document's id.
	 * @param lemma the specified lemma.
	 * @return a double.
	 */
	double get_termFrequency_logNorm (String documentId, String lemma);
	
	/**
	 * Compute and return term-frequencies for all lemmata in the specified document. 
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double term-frequency).
	 */
	HashMap<String, Double> get_termFrequencies (String documentId);
	
	/**
	 * The amount of documents that contain at least one instance of <i>lemma</i>.
	 * @param lemma the specified lemma.
	 * @return a double (cast from int).
	 */
	double get_documentsContaining (String lemma);
	
	/**
	 * The natural logarithm from the division of the count of documents and the documents containing <i>lemma</i>.  
	 * @param lemma the specified lemma.
	 * @return a double.
	 */
	double get_inverseDocumentFrequency (String lemma);

	/**
	 * Inverse-document-frequencies for all lemmata in the specified document.
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double inverse-document-frequency).
	 */
	HashMap<String, Double> get_inverseDocumentFrequencies (String documentId);
	
	/**
	 * The TF-IDF for <i>lemma</i> in the specified document.
	 * @param documentId the specified document's id.
	 * @param lemma the specified lemma.
	 * @return a double.
	 */
	double get_tfidf (String documentId, String lemma);
	
	/**
	 * The TF-IDF for <b>all lemmata</b> in the specified document.
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double tfidf).
	 */
	HashMap<String, Double> get_tfidf_all (String documentId);
	
	/**
	 * The TF-IDF for <b>all lemmata</b> in <i>all</i> documents. <b>Warning:</b> May produce heavy server load.
	 * @return a HashMap(String documentId, HashMap(String lemma, Double tfidf)).
	 */
	HashMap<String, HashMap<String, Double>> get_tfidf_all_all ();

	
	//---------------------------------------------------------------------------
	// Utility
	//---------------------------------------------------------------------------

	/**
	 * A set of all lemmata in the specified document.
	 * @param documentId the specified document's id.
	 * @return a HashSet(String lemma).
	 */
	HashSet<String> get_Lemmata (String documentId);
	
	/**
	 * <p>A List of all token-bi-grams in the specified document.</p>
	 * The bi-gram is represented by a string, which is the product
	 * of the concatenation of both parts of the bi-gramm, spepareted by a '-'.
	 * @param documentId the specified document's id.
	 * @return an ArrayList(String bi-gram)
	 * @throws UnsupportedOperationException if the database does not support this operation.
	 */
	ArrayList<String> get_bi_grams(String documentId) throws UnsupportedOperationException;
	
	/**
	 * A list of all token-bi-grams from all documents. <b>Warning:</b> May produce heavy server load.
	 * See {@link #get_bi_grams get_bi_grams} for details.
	 * @return an ArrayList(String bi-gram).
	 * @throws UnsupportedOperationException if the database does not support this operation.
	 */
	ArrayList<String> get_bi_grams_all() throws UnsupportedOperationException;
	
	/**
	 * A list of all token-bi-grams from all documents specified in the collection. 
	 * See {@link #get_bi_grams get_bi_grams} for details.
	 * @param documentIds the specified document's id.
	 * @return an ArrayList(String bi-gram).
	 * @throws UnsupportedOperationException if the database does not support this operation.
	 */
	ArrayList<String> get_bi_grams_collection(Collection<String> documentIds)
			throws UnsupportedOperationException;
	
	/**
	 * <p>A List of all token-tri-grams in the specified document.</p>
	 * The tri-gram is represented by a string, which is the product
	 * of the concatenation of all parts of the tri-gramm, spepareted by a '-'.
	 * @param documentId the specified document's id.
	 * @return an ArrayList(String tri-gram)
	 * @throws UnsupportedOperationException if the database does not support this operation.
	 */
	ArrayList<String> get_tri_grams(String documentId) throws UnsupportedOperationException;
	
	/**
	 * A list of all token-tri-grams from all documents. <b>Warning:</b> May produce heavy server load.
	 * See {@link #get_tri_grams get_tri_grams} for details.
	 * @return an ArrayList(String tri-gram).
	 * @throws UnsupportedOperationException if the database does not support this operation.
	 */
	ArrayList<String> get_tri_grams_all() throws UnsupportedOperationException;
	
	/**
	 * A list of all token-tri-grams from all documents specified in the collection. 
	 * See {@link #get_tri_grams get_tri_grams} for details.
	 * @param documentIds the specified documents id's.
	 * @return an ArrayList(String tri-gram).
	 * @throws UnsupportedOperationException if the database does not support this operation.
	 */
	ArrayList<String> get_tri_grams_collection(Collection<String> documentIds)
			throws UnsupportedOperationException;
}
