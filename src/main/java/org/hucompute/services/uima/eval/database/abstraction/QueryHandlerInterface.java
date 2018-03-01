package org.hucompute.services.uima.eval.database.abstraction;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;
import org.hucompute.services.uima.eval.database.abstraction.implementation.*;
import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.implementation.*;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The interface for all relevant server queries.
 * <p>
 * Mixes database abstraction and a little bit of uima logic, since the JCas
 * format is required as an input and output format.
 * <p>
 * The insertion methods are not recursive. I.e. the storeJCasDocument method
 * should only store the document itself, and not any sub-elements (like para-
 * graphs). For that an extenal control structure has to iterate over the docu-
 * ment and call the appropriate methods.
 *
 * @author Manuel Stoeckel
 * @author Hannes Leutloff <hannes.leutloff@aol.de>
 * Created on 26.09.2017
 */
public interface QueryHandlerInterface
{
	//--------------------------------------------------------------------------
	// Factories
	//--------------------------------------------------------------------------

	/**
	 * Creates an instance of a QueryHandlerInterface implementation based on
	 * the connection supplied.
	 *
	 * @param aConnection The connection for which a handler should be created.
	 * @return A fully initialized QueryHandlerInterface instance.
	 */
	static QueryHandlerInterface createQueryHandlerForConnection(
			Connection aConnection
	)
	{
		QueryHandlerInterface queryhandler = null;
		if (aConnection.getClass() == ArangoDBConnection.class)
		{
			queryhandler = new ArangoDBQueryHandler(
					((ArangoDBConnection) aConnection).getArangoDB()
			);
		} else if (aConnection.getClass() == BaseXConnection.class)
		{
			queryhandler = new BaseXQueryHandler(
					((BaseXConnection) aConnection).getSession()
			);
		} else if (aConnection.getClass() == CassandraConnection.class)
		{
			queryhandler = new CassandraQueryHandler(
					((CassandraConnection) aConnection).getSession()
			);
		} else if (aConnection.getClass() == MongoDBConnection.class)
		{
			queryhandler = new MongoDBQueryHandler(
					((MongoDBConnection) aConnection).getClient()
			);
		} else if (aConnection.getClass() == MySQLConnection.class)
		{
			queryhandler = new MySQLQueryHandler(
					((MySQLConnection) aConnection).getConnection()
			);
		} else if (aConnection.getClass() == Neo4jConnection.class)
		{
			queryhandler = new Neo4jQueryHandler(
					((Neo4jConnection) aConnection).getDriver()
			);
		}

		return queryhandler;
	}

	//--------------------------------------------------------------------------
	// Setup
	//--------------------------------------------------------------------------

	/**
	 * Prepares any necessary structures in the database and makes sure, that
	 * all queries can be executed without structural error.
	 * <p>
	 * This may rebuild the database and should result in an empty database.
	 */
	void setUpDatabase() throws IOException;

	/**
	 * Clears the database from any content. However, it leaves any necessary
	 * structures intact.
	 */
	void clearDatabase() throws IOException;

	//--------------------------------------------------------------------------
	// Raw Querying
	//
	// Meaning everything that has no analysis directly attached. Counting ele-
	// ments is not interpreted as analysis.
	// This includes read and write operations.
	//--------------------------------------------------------------------------

	/**
	 * Stores the full Document hierarchy.
	 * This includes iteration over Paragraphs, Sentences etc.
	 *
	 * @param document The Document to insert.
	 * @throws QHException If anything goes wrong. Encapsulates underlying
	 *                     Exceptions.
	 */
	void storeDocumentHierarchy(JCas document) throws QHException;

	/**
	 * Stores a JCas Document in an appropriate way.
	 *
	 * @param document The JCas Document.
	 * @return The Document's id.
	 */
	String storeJCasDocument(JCas document) throws QHException;

	/**
	 * Stores more than one JCas Document at once.
	 * Difference to multiple #storeJCasDocument calls could optionally be im-
	 * proved performance on some systems.
	 *
	 * @param documents An iterable object of Documents.
	 * @return The Documents' ids.
	 */
	Iterable<String> storeJCasDocuments(Iterable<JCas> documents) throws QHException;

	/**
	 * Stores a Paragraph in the database.
	 *
	 * @param paragraph           The Paragraph.
	 * @param documentId          The id of the document in which the paragraph
	 *                            occurs.
	 * @param previousParagraphId The predecessing Paragraph's id.
	 * @return The Paragraph's id.
	 */
	String storeParagraph(
			Paragraph paragraph,
			String documentId,
			String previousParagraphId
	);

	/**
	 * Stores a Paragraph in the database.
	 *
	 * @param paragraph  The Paragraph.
	 * @param documentId The id of the document in which the paragraph
	 *                   occurs.
	 * @return The Paragraph's id.
	 */
	String storeParagraph(Paragraph paragraph, String documentId);

	/**
	 * Stores a Sentence in the database.
	 *
	 * @param sentence           The Sentence.
	 * @param documentId         The id of the document in which the paragraph
	 *                           occurs.
	 * @param paragraphId        The id of the Paragraph in which the Sentence
	 *                           occurs.
	 * @param previousSentenceId The predecessing Sentence's id.
	 * @return The Sentence's id.
	 */
	String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId,
			String previousSentenceId
	);

	/**
	 * Stores a Sentence in the database.
	 *
	 * @param sentence    The Sentence.
	 * @param documentId  The id of the document in which the paragraph
	 *                    occurs.
	 * @param paragraphId The id of the Paragraph in which the Sentence occurs.
	 * @return The Sentence's id.
	 */
	String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId
	);

	/**
	 * Stores a Token in the database.
	 *
	 * @param token           The Token.
	 * @param documentId      The id of the document in which the paragraph
	 *                        occurs.
	 * @param paragraphId     The id of the Paragraph in which the Sentence
	 *                        occurs.
	 * @param sentenceId      The id of the Sentence in which the Token occurs.
	 * @param previousTokenId The predecessing Token's id.
	 * @return The Token's id.
	 */
	String storeToken(
			Token token,
			String documentId,
			String paragraphId,
			String sentenceId,
			String previousTokenId
	);

	/**
	 * Stores a Token in the database.
	 *
	 * @param token       The Token.
	 * @param documentId  The id of the document in which the paragraph
	 *                    occurs.
	 * @param paragraphId The id of the Paragraph in which the Sentence
	 *                    occurs.
	 * @param sentenceId  The id of the Sentence in which the Token occurs.
	 * @return The Token's id.
	 */
	String storeToken(
			Token token,
			String documentId,
			String paragraphId,
			String sentenceId
	);

	/**
	 * @throws DocumentNotFoundException If the given documentId was not found
	 *                                   in the database.
	 */
	void checkIfDocumentExists(String documentId)
			throws DocumentNotFoundException;

	/**
	 * Return the ids of all documents currently stored.
	 *
	 * @return The ids of all Documents stored in the database.
	 */
	Iterable<String> getDocumentIds();

	/**
	 * A set of all lemmata in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashSet(String lemma).
	 */
	Set<String> getLemmataForDocument(String documentId)
			throws DocumentNotFoundException;

	/**
	 * Retrieves all stored objects in JCas format.
	 * Produces a Document -> Token -> Lemma/Pos structure. No paragraphs and
	 * sentences anymore.
	 *
	 * @param aCAS       The CAS to populate with the found data.
	 * @param documentId The document whose data shall be used.
	 * @throws DocumentNotFoundException If the documentId can't be found in db.
	 * @throws QHException               If any underlying Exception is thrown.
	 */
	void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException;

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
	int countElementsOfType(ElementType type) throws TypeNotCountableException;

	/**
	 * Counts all elements of <i>type</i> in the specified document.
	 *
	 * @param documentId The id of the document from which elements are to be
	 *                   counted.
	 * @param type       Instance of ElementType, namely Token, Lemma, Pos,
	 *                   Document, sentence or Paragraph.
	 * @return An integer.
	 * @throws DocumentNotFoundException if the documentId can't be found in db
	 * @throws TypeNotCountableException if the given type can't be counted.
	 */
	int countElementsInDocumentOfType(String documentId, ElementType type)
			throws DocumentNotFoundException, TypeNotCountableException;

	/**
	 * Counts all elements of <i>type</i> across all documents with given
	 * <i>value</i>.
	 *
	 * @param type  Instance of Const.TYPE, namely TOKEN, LEMMA or POS.
	 * @param value String, value of the element.
	 * @return An integer.
	 * @throws TypeHasNoValueException when the <i>type</i>
	 *                                 given does not match TOKEN, LEMMA or
	 *                                 POS.
	 */
	int countElementsOfTypeWithValue(ElementType type, String value)
			throws TypeNotCountableException, TypeHasNoValueException;

	/**
	 * Counts all elements of <i>type</i> within one specified document with gi-
	 * ven <i>value</i>.
	 *
	 * @param documentId The id of the document from which elements are to be
	 *                   counted.
	 * @param type       Instance of Const.TYPE, namely TOKEN, LEMMA or POS.
	 * @param value      String, value of the element.
	 * @return An integer.
	 * @throws TypeHasNoValueException when the <i>type</i>
	 *                                 given does not match TOKEN, LEMMA or
	 *                                 POS.
	 */
	int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException, TypeNotCountableException,
			TypeHasNoValueException;

	/**
	 * Returns for each Lemma the amount of connections of the form
	 * Document->Token->Lemma. Since usually there is always a maximum of one
	 * connection from a Document to a Lemma (since Lemmata are Singletons and
	 * connections treated as such), this needs to take the indirection via To-
	 * kens.
	 *
	 * @return Map from lemma value to occurence count.
	 */
	Map<String, Integer> countOccurencesForEachLemmaInAllDocuments();

	//--------------------------------------------------------------------------
	// Structure
	//
	// TODO: add index related methods
	//--------------------------------------------------------------------------

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
	Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException;

	/**
	 * Get TTR for all specified documents.
	 * Documents that can't be found are skipped.
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
	 * Computes the term frequency without norming for each lemma in the speci-
	 * fied document.
	 *
	 * @param documentId The document to calculate frequencies for.
	 * @return a map from lemma to frequency
	 */
	Map<String, Integer> calculateRawTermFrequenciesInDocument(
			String documentId
	) throws DocumentNotFoundException;

	/**
	 * Computes the term frequency without norming for the given lemma in the
	 * specified document.
	 *
	 * @param lemma      The lemma to search for.
	 * @param documentId The document to calculate frequencies for.
	 * @return the lemma's term frequency
	 */
	Integer calculateRawTermFrequencyForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException;

	/**
	 * The term-frequency normed with 0.5 (a double).
	 *
	 * @param lemma      the specified lemma.
	 * @param documentId the specified document's id.
	 * @return a double.
	 */
	double calculateTermFrequencyWithDoubleNormForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException;

	/**
	 * The term-frequency normed with the natural logarithm.
	 *
	 * @param lemma      the specified lemma.
	 * @param documentId the specified document's id.
	 * @return a double.
	 */
	double calculateTermFrequencyWithLogNormForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException;

	/**
	 * Compute and return term-frequencies for all lemmata in the specified
	 * document.
	 * Per default this should use a double normalization with parameter 0.5.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double term-frequency).
	 */
	Map<String, Double> calculateTermFrequenciesForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException;

	/**
	 * The natural logarithm from the division of the count of documents and the
	 * documents containing <i>lemma</i>.
	 * <p>
	 * Returns 0 if the lemma is not found in the database.
	 *
	 * @param lemma the specified lemma.
	 * @return a double.
	 */
	double calculateInverseDocumentFrequency(String lemma)
			throws OperationNotSupportedException;

	/**
	 * Inverse-document-frequencies for all lemmata in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double inverse-document-frequency).
	 */
	Map<String, Double>
	calculateInverseDocumentFrequenciesForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException, OperationNotSupportedException;

	/**
	 * The TF-IDF for <i>lemma</i> in the specified document.
	 * <p>
	 * Returns 0 if the lemma is not found in the document.
	 *
	 * @param lemma      the specified lemma.
	 * @param documentId the specified document's id.
	 * @return a double.
	 */
	double calculateTFIDFForLemmaInDocument(String lemma, String documentId)
			throws DocumentNotFoundException, OperationNotSupportedException;

	/**
	 * The TF-IDF for <b>all lemmata</b> in the specified document.
	 *
	 * @param documentId the specified document's id.
	 * @return a HashMap(String lemma, Double tfidf).
	 */
	Map<String, Double> calculateTFIDFForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException, OperationNotSupportedException;

	/**
	 * The TF-IDF for <b>all lemmata</b> in <i>all</i> documents.
	 * <b>Warning:</b> May produce heavy server load.
	 *
	 * @return a map from document id to a map from lemma to tfidf.
	 */
	Map<String, Map<String, Double>>
	calculateTFIDFForLemmataInAllDocuments()
			throws OperationNotSupportedException;


	//--------------------------------------------------------------------------
	// Utility
	//
	// Bi-Grams are pairs of neighbouring tokens in a text.
	// Tri-Grams are thus triplets of neighbouring tokens in a text.
	//--------------------------------------------------------------------------

	/**
	 * A List of all token-bi-grams in the specified document.
	 * The bi-gram is represented by a string, which is the product
	 * of the concatenation of both parts of the bi-gramm, spepareted by a '-'.
	 *
	 * @param documentId the specified document's id.
	 * @return an Iterable of bi-grams.
	 * @throws DocumentNotFoundException     if the documentId can't be found in
	 *                                       the db.
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getBiGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException;

	/**
	 * A list of all token-bi-grams from all documents.
	 *
	 * @return an Iterable of bi-grams.
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException;

	/**
	 * A list of all token-bi-grams from all documents specified in the collec-
	 * tion.
	 * <p>
	 * If at least one of the documentIds is found, no Exception is thrown for
	 * other ids, which can't be found.
	 *
	 * @param documentIds the specified document's id.
	 * @return an Iterable of bi-grams.
	 * @throws DocumentNotFoundException     if none of the documentIds is
	 *                                       found.
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException;

	/**
	 * A List of all token-tri-grams in the specified document.
	 * The tri-gram is represented by a string, which is the product
	 * of the concatenation of all parts of the tri-gram, spepareted by '-'.
	 *
	 * @param documentId the specified document's id.
	 * @return an Iterable of tri-grams.
	 * @throws DocumentNotFoundException     if the documentId can't be found in
	 *                                       the db.
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException;

	/**
	 * A list of all token-tri-grams from all documents.
	 *
	 * @return an Iterable of tri-grams.
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException;

	/**
	 * A list of all token-tri-grams from all documents specified in the
	 * collection.
	 * <p>
	 * If at least one of the documentIds is found, no Exception is thrown for
	 * other ids, which can't be found.
	 *
	 * @param documentIds the specified document's ids.
	 * @return an Iterable of tri-grams.
	 * @throws DocumentNotFoundException     if none of the documentIds is
	 *                                       found.
	 * @throws UnsupportedOperationException if the database does not support
	 *                                       this operation.
	 */
	Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException;
}
