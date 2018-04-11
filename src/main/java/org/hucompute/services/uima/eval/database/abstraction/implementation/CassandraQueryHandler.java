package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.datastax.driver.core.*;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.connection.Connections;

import java.io.IOException;
import java.util.*;
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

		this.preparedStatementMap.put(
				"insertDocument",
				this.session.prepare(
						"INSERT INTO \"document\" " +
								"(\"uid\", \"text\", \"language\", " +
								"\"paragraphCount\", \"sentenceCount\", " +
								"\"tokenCount\", \"lemmaCount\", " +
								"\"posCount\") " +
								"VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertParagraph",
				this.session.prepare(
						"INSERT INTO \"paragraph\" " +
								"(\"uid\", \"documentId\", \"begin\", " +
								"\"end\", \"previousParagraphId\") " +
								"VALUES (?, ?, ?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertSentence",
				this.session.prepare(
						"INSERT INTO \"sentence\" " +
								"(\"uid\", \"documentId\", \"begin\", " +
								"\"end\", \"previousSentenceId\", " +
								"\"paragraphId\") " +
								"VALUES (?, ?, ?, ?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertToken",
				this.session.prepare(
						"INSERT INTO \"token\" " +
								"(\"documentId\", \"uid\", \"begin\", " +
								"\"end\", \"lemmaValue\", \"posValue\", " +
								"\"previousTokenId\", \"paragraphId\", " +
								"\"sentenceId\") " +
								"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"incrementLemmaCounter",
				this.session.prepare(
						"UPDATE \"lemma\" SET \"count\" = \"count\" + 1 " +
								"WHERE \"value\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"incrementPosCounter",
				this.session.prepare(
						"UPDATE \"pos\" SET \"count\" = \"count\" + 1 " +
								"WHERE \"value\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"incrementLemmaByDocumentCounter",
				this.session.prepare(
						"UPDATE \"lemmaByDocument\" " +
								"SET \"count\" = \"count\" + 1 " +
								"WHERE \"documentId\" = ? AND \"value\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"insertDocumentByLemma",
				this.session.prepare(
						"INSERT INTO \"documentByLemma\" " +
								"(\"value\", \"documentId\") " +
								"VALUES (?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertTokenByValue",
				this.session.prepare(
						"INSERT INTO \"tokenByValue\" " +
								"(\"lemmaValue\", \"tokenId\", " +
								"\"documentId\") " +
								"VALUES (?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"incrementPosByDocumentcounter",
				this.session.prepare(
						"UPDATE \"posByDocument\" " +
								"SET \"count\" = \"count\" + 1 " +
								"WHERE \"documentId\" = ? AND \"posValue\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"getTokenValue",
				this.session.prepare(
						"SELECT \"lemmaValue\" FROM \"token\" " +
								"WHERE \"documentId\" = ? AND \"uid\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"insertBiGram",
				this.session.prepare(
						"INSERT INTO \"bigram\" " +
								"(\"documentId\", \"firstValue\", " +
								"\"secondValue\") " +
								"VALUES (?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"getBiGramBySecondValue",
				this.session.prepare(
						"SELECT \"documentId\", \"firstValue\", " +
								"\"secondValue\" " +
								"FROM \"bigram\" " +
								"WHERE \"documentId\" = ? " +
								"AND \"secondValue\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"insertTriGram",
				this.session.prepare(
						"INSERT INTO \"trigram\" " +
								"(\"documentId\", \"firstValue\", " +
								"\"secondValue\", \"thirdValue\") " +
								"VALUES (?, ?, ?, ?);"
				)
		);

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
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				"  \"lemmaValue\" VARCHAR, " +
				"  \"posValue\" VARCHAR, " +
				"  \"previousTokenId\" VARCHAR, " +
				"  \"paragraphId\" VARCHAR, " +
				"  \"sentenceId\" VARCHAR, " +
				"  PRIMARY KEY (\"documentId\", \"uid\") " +
				")");

		session.execute("CREATE TABLE \"lemma\" ( " +
				"  \"value\" VARCHAR primary key, " +
				"  \"count\" counter " + // overall occurence count in all documents
				")");

		session.execute("CREATE TABLE \"pos\" ( " +
				"  \"value\" VARCHAR primary key, " +
				"  \"count\" counter " + // overall occurence count in all documents
				")");

		session.execute("CREATE TABLE \"lemmaByDocument\" ( " +
				"  \"documentId\" VARCHAR, " +
				"  \"value\" VARCHAR, " +
				"  \"count\" counter, " + // occurence count in said document
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
				"  \"count\" counter, " +
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
		session.execute("CREATE TABLE \"trigram\" ( " +
				"  \"documentId\" VARCHAR, " +
				"  \"firstValue\" VARCHAR, " +
				"  \"secondValue\" VARCHAR, " +
				"  \"thirdValue\" VARCHAR, " +
				"  PRIMARY KEY (\"documentId\", \"firstValue\", \"secondValue\", \"thirdValue\") " +
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
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		Set<String> posValues = new HashSet<>();
		Set<String> lemmaValues = new HashSet<>();

		// Count all the elements in the document
		int paragraphCount = 0;
		int sentenceCount = 0;
		int tokenCount = 0;
		for (Paragraph paragraph
				: JCasUtil.select(document, Paragraph.class))
		{
			paragraphCount++;
			for (Sentence sentence : JCasUtil.selectCovered(
					document,
					Sentence.class, paragraph
			))
			{
				sentenceCount++;
				for (Token token : JCasUtil.selectCovered(
						document, Token.class, sentence
				))
				{
					tokenCount++;
					lemmaValues.add(token.getLemma().getValue());
					posValues.add(token.getPos().getPosValue());
				}
			}
		}

		BoundStatement aStatement = this
				.preparedStatementMap.get("insertDocument").bind()
				.setString(0, documentId)
				.setString(1, document.getDocumentText())
				.setString(2, document.getDocumentLanguage())
				.setInt(3, paragraphCount)
				.setInt(4, sentenceCount)
				.setInt(5, tokenCount)
				.setInt(6, lemmaValues.size())
				.setInt(7, posValues.size());

		this.session.execute(aStatement);

		return documentId;
	}

	@Override
	public String storeParagraph(
			Paragraph paragraph,
			String documentId,
			String previousParagraphId
	)
	{
		String paragraphId = UUID.randomUUID().toString();

		BoundStatement aStatement = this
				.preparedStatementMap.get("insertParagraph").bind()
				.setString(0, paragraphId)
				.setString(1, documentId)
				.setInt(2, paragraph.getBegin())
				.setInt(3, paragraph.getEnd())
				.setString(4, previousParagraphId);

		this.session.execute(aStatement);

		return paragraphId;
	}

	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId,
			String previousSentenceId
	)
	{
		String sentenceId = UUID.randomUUID().toString();

		BoundStatement aStatement = this
				.preparedStatementMap.get("insertSentence").bind()
				.setString(0, sentenceId)
				.setString(1, documentId)
				.setInt(2, sentence.getBegin())
				.setInt(3, sentence.getEnd())
				.setString(4, previousSentenceId)
				.setString(5, paragraphId);

		this.session.execute(aStatement);

		return sentenceId;
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
		String tokenId = UUID.randomUUID().toString();

		String lemmaValue = token.getLemma().getValue();
		String posValue = token.getPos().getPosValue();

		// Insert Token.
		BoundStatement aStatement = this
				.preparedStatementMap.get("insertToken").bind()
				.setString(0, documentId)
				.setString(1, tokenId)
				.setInt(2, token.getBegin())
				.setInt(3, token.getEnd())
				.setString(4, lemmaValue)
				.setString(5, posValue)
				.setString(6, previousTokenId)
				.setString(7, paragraphId)
				.setString(8, sentenceId);
		this.session.execute(aStatement);

		// Increment occurence counter for Lemma. (Inserts it, if it doesn't
		// exist yet.)
		aStatement = this
				.preparedStatementMap.get("incrementLemmaCounter").bind()
				.setString(0, lemmaValue);
		this.session.execute(aStatement);

		// Increment occurence counter for POS. (Inserts it, if it doesn't exist
		// yet.)
		aStatement = this
				.preparedStatementMap.get("incrementPosCounter").bind()
				.setString(0, posValue);
		this.session.execute(aStatement);

		// Increment occurence counter for Lemma in current Document. (Inserts
		// it, if it doesn't exist yet.)
		aStatement = this
				.preparedStatementMap.get("incrementLemmaByDocumentCounter")
				.bind()
				.setString(0, documentId)
				.setString(1, lemmaValue);
		this.session.execute(aStatement);

		// Insert DocumentByLemma reference.
		aStatement = this
				.preparedStatementMap.get("insertDocumentByLemma").bind()
				.setString(0, lemmaValue)
				.setString(1, documentId);
		this.session.execute(aStatement);

		// Insert TokenByValue reference.
		aStatement = this
				.preparedStatementMap.get("insertTokenByValue").bind()
				.setString(0, lemmaValue)
				.setString(1, tokenId)
				.setString(2, documentId);
		this.session.execute(aStatement);

		// Increment PosByDocument counter. (Inserts it, if it doesn't exist
		// yet.)
		aStatement = this
				.preparedStatementMap.get("incrementPosByDocumentcounter")
				.bind()
				.setString(0, documentId)
				.setString(1, posValue);
		this.session.execute(aStatement);

		if (previousTokenId != null) {
			// Get value of previous Token for insertion into bigram.
			aStatement = this.preparedStatementMap.get("getTokenValue").bind()
					.setString(0, documentId)
					.setString(1, previousTokenId);
			ResultSet result = this.session.execute(aStatement);
			String previousTokenValue = result.one().getString(0);

			// Insert bigram with previous and current Token.
			aStatement = this.preparedStatementMap.get("insertBiGram").bind()
					.setString(0, documentId)
					.setString(1, previousTokenValue)
					.setString(2, lemmaValue);
			this.session.execute(aStatement);

			// Check if previous Token was already second value in a bigram.
			aStatement = this
					.preparedStatementMap.get("getBiGramBySecondValue").bind()
					.setString(0, documentId)
					.setString(1, previousTokenValue);
			result = this.session.execute(aStatement);
			Row row = result.one();
			if (row != null) {
				// If the previous Token was the second value in a trigram, it
				// and its previous Token can be used in a new trigram.
				String prevPreviousTokenValue = row.getString(1);

				aStatement = this
						.preparedStatementMap.get("insertTriGram").bind()
						.setString(0, documentId)
						.setString(1, prevPreviousTokenValue)
						.setString(2, previousTokenValue)
						.setString(3, lemmaValue);
				this.session.execute(aStatement);
			}
		}

		return tokenId;
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
