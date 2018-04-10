package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.connection.Connections;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Most of the queries here use the same concept as in the MySQLQueryHandler.
 * For more information on certains methods, look there.
 */
public class CassandraQueryHandler extends AbstractQueryHandler
{
	protected Session session;
	protected boolean statementsPrepared = false;
	protected Map<String, PreparedStatement> preparedStatementMap;

	public CassandraQueryHandler(Session session)
	{
		this.session = session;

		this.preparedStatementMap = new HashMap<>();
	}

	/**
	 * Prepares often use statements, so they are only defined once.
	 * <p>
	 * Has to be executed after a keyspace was selected via a "USE" command AND
	 * the database is completely set up.
	 */
	protected void prepareStatements()
	{
		if (this.statementsPrepared)
		{
			return;
		}

		this.statementsPrepared = true;
	}

	protected PreparedStatement getOrPrepare(String name, String query)
	{
		if (this.preparedStatementMap.containsKey(name))
		{
			return this.preparedStatementMap.get(name);
		}
		PreparedStatement preparedStatement = this.session.prepare(query);
		this.preparedStatementMap.put(
				name,
				preparedStatement
		);
		return preparedStatement;
	}

	;

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.Cassandra;
	}

	/**
	 * Primary keys are automatically indexed with a primary index.
	 * Secondary indexes are created manually for all fields that are queried at
	 * some point. If there were no secondary indexes, cassandra would complain
	 * that the queries have unpredictable performance.
	 */
	@Override
	public void setUpDatabase()
	{
		session.execute("CREATE KEYSPACE IF NOT EXISTS " +
				System.getenv("CASSANDRA_DB") + " WITH replication = {" +
				"'class':'SimpleStrategy'," +
				"'replication_factor':1" +
				"};");

		session.execute("USE " + System.getenv("CASSANDRA_DB"));

		session.execute("DROP TABLE IF EXISTS \"document\"");
		session.execute("DROP TABLE IF EXISTS \"paragraph\"");
		session.execute("DROP TABLE IF EXISTS \"sentence\"");
		session.execute("DROP TABLE IF EXISTS \"token\"");
		session.execute("DROP TABLE IF EXISTS \"lemma\"");
		session.execute("DROP TABLE IF EXISTS \"pos\"");
		session.execute("DROP TABLE IF EXISTS \"lemmaByDocument\"");
		session.execute("DROP TABLE IF EXISTS \"documentByLemma\"");
		session.execute("DROP TABLE IF EXISTS \"tokenByValue\"");
		session.execute("DROP TABLE IF EXISTS \"posByDocument\"");
		session.execute("DROP TABLE IF EXISTS \"bigram\"");
		session.execute("DROP TABLE IF EXISTS \"trigram\"");

		session.execute("CREATE TABLE \"document\" ( " +
				"  \"uid\" VARCHAR primary key, " + // documentId
				"  \"text\" VARCHAR, " +
				"  \"language\" VARCHAR, " +
				"  \"paragraphCount\" INT, " +
				"  \"sentenceCount\" INT, " +
				"  \"tokenCount\" INT, " +
				"  \"lemmaCount\" INT, " +
				"  \"posCount\" INT " +
				")");

		session.execute("CREATE TABLE \"paragraph\" ( " +
				"  \"uid\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				"  \"previousParagraphId\" VARCHAR, " +
				"  PRIMARY KEY (\"documentId\", \"uid\") " +
				")");

		session.execute("CREATE TABLE \"sentence\" ( " +
				"  \"documentId\" VARCHAR, " +
				"  \"uid\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				"  \"previousSentenceId\" VARCHAR, " +
				"  \"paragraphId\" VARCHAR, " +
				"  PRIMARY KEY (\"documentId\", \"uid\") " +
				")");

		session.execute("CREATE TABLE \"token\" ( " +
				"  \"documentId\" VARCHAR, " +
				"  \"uid\" VARCHAR, " +
				"  \"lemmaValue\" VARCHAR, " +
				"  \"posValue\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				"  \"previousTokenId\" VARCHAR, " +
				"  \"paragraphId\" VARCHAR, " +
				"  \"sentenceId\" VARCHAR, " +
				"  PRIMARY KEY (\"documentId\", \"uid\") " +
				")");

		session.execute("CREATE TABLE \"lemma\" ( " +
				"  \"value\" VARCHAR primary key, " +
				"  \"count\" INT " + // overall occurence count in all documents
				")");

		session.execute("CREATE TABLE \"pos\" ( " +
				"  \"value\" VARCHAR primary key, " +
				"  \"count\" VARCHAR " + // overall occurence count in all documents
				")");

		session.execute("CREATE TABLE \"lemmaByDocument\" ( " +
				"  \"documentId\" VARCHAR, " +
				"  \"value\" VARCHAR, " +
				"  \"count\" INT, " + // occurence count in said document
				"  PRIMARY KEY (\"documentId\", \"value\") " +
				")");

		session.execute("CREATE TABLE \"documentByLemma\" ( " +
				"  \"value\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  PRIMARY KEY (\"value\", \"documentId\") " +
				")");

		session.execute("CREATE TABLE \"tokenByValue\" ( " +
				"  \"lemmaValue\" VARCHAR, " +
				"  \"tokenId\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  PRIMARY KEY (\"lemmaValue\", \"tokenId\", \"documentId\") " +
				")");

		session.execute("CREATE TABLE \"posByDocument\" ( " +
				"  \"documentId\" VARCHAR, " +
				"  \"posValue\" VARCHAR, " +
				"  \"count\" INT, " +
				"  PRIMARY KEY (\"documentId\", \"posValue\") " +
				")");

		/*
		 * Primary key is documentId and secondValue so that bigrams can be
		 * looked up by second value.
		 * This is important while inserting trigrams: when checking if two
		 * values are part of a trigram, the first of those values can be looked
		 * up here. If a bigram with the first value on second position exists,
		 * the found bigram plus the second value is a new trigram.
		 */
		session.execute("CREATE TABLE \"bigram\" ( " +
				"  \"documentId\" VARCHAR, " +
				"  \"firstValue\" VARCHAR, " +
				"  \"secondValue\" VARCHAR, " +
				"  PRIMARY KEY ((\"documentId\", \"secondValue\"), \"firstValue\") " +
				")");

		this.prepareStatements();
	}

	@Override
	public void openDatabase() throws IOException
	{
		session.execute("USE " + System.getenv("CASSANDRA_DB"));
		this.prepareStatements();
	}

	@Override
	public void clearDatabase()
	{
		this.setUpDatabase();
	}

	@Override
	public String storeJCasDocument(JCas document)
	{
		return null;
	}

	@Override
	public String storeParagraph(
			Paragraph paragraph,
			String documentId,
			String previousParagraphId
	)
	{
		return null;
	}

	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId,
			String previousSentenceId
	)
	{
		return null;
	}

	@Override
	public String storeToken(
			Token token,
			String documentId,
			String paragraphId,
			String sentenceId,
			String previousTokenId
	)
	{
		return null;
	}

	/**
	 * Creates a new Lemma if none with the given value exists.
	 *
	 * @param value The Lemma's value.
	 * @return The Lemma's id.
	 */
	protected String getLemmaId(String value)
	{
		return null;
	}

	/**
	 * Inserts a connection from Document to Lemma into the table
	 * documentLemmaMap.
	 * Since Cassandra is more efficient on write, the existence of the connec-
	 * tion is not checked first. This operation is idempotent anyway.
	 *
	 * @param documentId
	 * @param lemmaId
	 */
	protected void insertDocumentLemmaConnection(
			String documentId, String lemmaId
	)
	{

	}

	@Override
	public void checkIfDocumentExists(String documentId) throws DocumentNotFoundException
	{

	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		return null;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
			throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		return null;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{
		this.checkIfDocumentExists(documentId);
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		return 0;
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		return 0;
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		// => type is either Token, Lemma or POS.

		return 0;
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		this.checkIfDocumentExists(documentId);
		// => type is either Token, Lemma or POS.

		return 0;
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		return null;
	}

	protected class DocumentTokenLemma
	{
		public String document;
		public String token;
		public String lemma;

		public DocumentTokenLemma(
				String document,
				String token,
				String lemma
		)
		{
			this.document = document;
			this.token = token;
			this.lemma = lemma;
		}

		public String getDocument()
		{
			return this.document;
		}

		public String getLemma()
		{
			return this.lemma;
		}
	}

	protected Map<String, Double> calculateTTRForDocumentTokenLemmaMap(
			Map<String, DocumentTokenLemma> dttMap
	)
	{
		Map<String, Double> ttrMap = new HashMap<>();

		dttMap.entrySet()
				.parallelStream()
				.collect(
						Collectors.groupingByConcurrent(Map.Entry::getKey,
								Collectors.mapping(
										Map.Entry::getValue,
										Collectors.toSet()
								)
						)
				)
				.forEach((document, list) -> {
					int tokenCount = list.size();
					int lemmaCount = (int) list.parallelStream()
							.map(DocumentTokenLemma::getLemma)
							.distinct()
							.count();

					ttrMap.put(
							document,
							(double) tokenCount / (double) lemmaCount
					);
				});

		return ttrMap;
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		return null;
	}

	@Override
	public Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(
			Collection<String> documentIds
	)
	{
		return null;
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		return null;
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		return 0;
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}
}
