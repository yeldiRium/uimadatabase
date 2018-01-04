package dbtest.queryHandler;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.jcas.JCas;

import java.util.*;

/**
 * The interface for all relevant server queries.
 * <p>
 * Mixes database abstraction and a little bit of uima logic, since the JCas
 * format is required as an input and output format.
 * <p>
 * Maybe an abstraction can simplify the process of extracting information from
 * JCas objects and creating them. TODO: elaborate and update.
 *
 * @author Manuel Stoeckel
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 * Created on 26.09.2017
 */
public interface QueryHandlerInterface
{
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
	Set<String> getLemmataForDocument(String documentId);

	/**
	 * Stores a JCas document in an appropriate way.
	 *
	 * @param document The JCas document.
	 */
	void storeJCasDocument(JCas document);

	/**
	 * Stores a Paragraph in the database.
	 *
	 * @param paragraph         The Paragraph.
	 * @param document          The document in which the paragraph occurs.
	 * @param previousParagraph The predecessing Paragraph.
	 */
	void storeParagraph(Paragraph paragraph, JCas document, Paragraph previousParagraph);

	/**
	 * Stores a Paragraph in the database.
	 *
	 * @param paragraph The Paragraph.
	 * @param document  The document in which the paragraph occurs.
	 */
	void storeParagraph(Paragraph paragraph, JCas document);

	/**
	 * Stores a Sentence in the database.
	 *
	 * @param sentence         The Sentence.
	 * @param document         The Document in which the entence occurs.
	 * @param paragraph        The Paragraph, in which the Sentence occurs.
	 * @param previousSentence The predecessing Sentence.
	 */
	void storeSentence(Sentence sentence, JCas document, Paragraph paragraph, Sentence previousSentence);

	/**
	 * Stores a Sentence in the database.
	 *
	 * @param sentence  The Sentence.
	 * @param document  The Document in which the entence occurs.
	 * @param paragraph The Paragraph, in which the Sentence occurs.
	 */
	void storeSentence(Sentence sentence, JCas document, Paragraph paragraph);

	/**
	 * Stores a Token in the database.
	 *
	 * @param token         The Token.
	 * @param document      The Document in which the Token occurs.
	 * @param paragraph     The Paragraph, in which the Token occurs.
	 * @param sentence      The Sentence, in which the Token occurs.
	 * @param previousToken The predecessing Token.
	 */
	void storeToken(Token token, JCas document, Paragraph paragraph, Sentence sentence, Token previousToken);

	/**
	 * Stores a Token in the database.
	 *
	 * @param token     The Token.
	 * @param document  The Document in which the Token occurs.
	 * @param paragraph The Paragraph, in which the Token occurs.
	 * @param sentence  The Sentence, in which the Token occurs.
	 */
	void storeToken(Token token, JCas document, Paragraph paragraph, Sentence sentence);

	/**
	 * Stores more than one JCas document at once.
	 * Difference to multiple #storeJCasDocument calls could optionally be im-
	 * proved performance on some systems.
	 *
	 * @param documents An iterable object of documents.
	 */
	void storeJCasDocuments(Iterable<JCas> documents);

	/**
	 * Retrieves all stored objects in JCas format.
	 */
	void getDocumentsAsJCas();

	// TODO: add some query options to document retrieval

	/**
	 * The amount of documents that contain at least one instance of
	 * <i>lemma</i>.
	 *
	 * @param lemma the specified lemma.
	 * @return a double (cast from int).
	 */
	int countDocumentsContainingLemma(String lemma);

	/**
	 * Count all elements of the specified <i>type</i>.
	 *
	 * @param type Instance of Const.TYPE, namely TOKEN, LEMMA, POS,
	 *             DOCUMENT, SENTENCE or PARAGRAPH.
	 * @return An integer.
	 */
	int countElementsOfType(ElementType type);

	/**
	 * Counts all elements of <i>type</i> in the specified document.
	 *
	 * @param documentId The id of the document from which elements are to be
	 *                   counted.
	 * @param type       Instance of Const.TYPE, namely TOKEN, LEMMA, POS,
	 *                   DOCUMENT, SENTENCE or PARAGRAPH.
	 * @return An integer.
	 */
	int countElementsInDocumentOfType(String documentId, ElementType type);

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
	int countElementsOfTypeWithValue(ElementType type, String value)
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
	int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
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
	Map<String, Double> calculateTTRForAllDocuments();

	/**
	 * Get TTR for the specified document.
	 *
	 * @param documentId The id of the document to get the TTR from.
	 * @return Map(String DocumentId, Double TTR - value)
	 */
	Map<String, Double> calculateTTRForDocument(String documentId);

	/**
	 * Get TTR for all specified documents.
	 *
	 * @param documentIds The id's of the document to get the TTR from.
	 * @return Map(String DocumentId, Double TTR - value)
	 */
	Map<String, Double> calculateTTRForCollectionOfDocuments(
			Collection<String> documentIds
	);


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
	 * @param lemma      the specified lemma.
	 * @param documentId the specified document's id.
	 * @return a double.
	 */
	double calculateTermFrequencyWithDoubleNormForLemmaInDocument(
			String lemma, String documentId
	);

	/**
	 * The term-frequency normed with the natural logarithm.
	 *
	 * @param lemma      the specified lemma.
	 * @param documentId the specified document's id.
	 * @return a double.
	 */
	double calculateTermFrequencyWithLogNermForLemmaInDocument(
			String lemma, String documentId
	);

	/**
	 * Compute and return term-frequencies for all lemmata in the specified
	 * document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double term-frequency).
	 */
	Map<String, Double> calculateTermFrequenciesForLemmataInDocument(
			String documentId
	);

	/**
	 * The natural logarithm from the division of the count of documents and the
	 * documents containing <i>lemma</i>.
	 *
	 * @param lemma the specified lemma.
	 * @return a double.
	 */
	double calculateInverseDocumentFrequency(String lemma);

	/**
	 * Inverse-document-frequencies for all lemmata in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double inverse-document-frequency).
	 */
	Map<String, Double>
	calculateInverseDocumentFrequenciesForLemmataInDocument(
			String documentId
	);

	/**
	 * The TF-IDF for <i>lemma</i> in the specified document.
	 *
	 * @param lemma      the specified lemma.
	 * @param documentId the specified document's id.
	 * @return a double.
	 */
	double calculateTFIDFForLemmaInDocument(String lemma, String documentId);

	/**
	 * The TF-IDF for <b>all lemmata</b> in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double tfidf).
	 */
	Map<String, Double> calculateTFIDFForLemmataInDocument(
			String documentId
	);

	/**
	 * The TF-IDF for <b>all lemmata</b> in <i>all</i> documents.
	 * <b>Warning:</b> May produce heavy server load.
	 *
	 * @return a map from document id to a map from lemma to tfidf.
	 */
	Map<String, Map<String, Double>>
	calculateTFIDFForLemmataInAllDocuments();


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
	Iterable<String> getBiGramsFromDocument(String documentId)
			throws UnsupportedOperationException;

	/**
	 * A list of all token-bi-grams from all documents.
	 * <b>Warning:</b> May produce heavy server load.
	 * See {@link #getBiGramsFromDocument getBiGramsFromDocument} for details.
	 *
	 * @return an ArrayList(String bi-gram).
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException;

	/**
	 * A list of all token-bi-grams from all documents specified in the collec-
	 * tion.
	 * See {@link #getBiGramsFromDocument getBiGramsFromDocument} for details.
	 *
	 * @param documentIds the specified document's id.
	 * @return an ArrayList(String bi-gram).
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException;

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
	Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException;

	/**
	 * A list of all token-tri-grams from all documents. <b>Warning:</b> May
	 * produce heavy server load.
	 * See {@link #getTriGramsFromDocument getTriGramsFromDocument} for details.
	 *
	 * @return an ArrayList(String tri-gram).
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException;

	/**
	 * A list of all token-tri-grams from all documents specified in the
	 * collection.
	 * See {@link #getTriGramsFromDocument getTriGramsFromDocument} for details.
	 *
	 * @param documentIds the specified documents id's.
	 * @return an ArrayList(String tri-gram).
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException;
}
