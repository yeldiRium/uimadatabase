package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.datastax.driver.core.*;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
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
				"createDocument",
				this.session.prepare(
						"INSERT INTO \"" + ElementType.Document + "\"" +
								" (\"uid\", \"text\", \"language\")" +
								" VALUES (?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertParagraph",
				this.session.prepare(
						"INSERT INTO \"" + ElementType.Paragraph + "\"" +
								" (\"uid\", \"documentId\", \"previousParagraphId\", \"begin\", \"end\")" +
								" VALUES (?, ?, ?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertSentence",
				this.session.prepare(
						"INSERT INTO \"" + ElementType.Sentence + "\"" +
								" (\"uid\", \"documentId\", \"paragraphId\", \"previousSentenceId\", \"begin\", \"end\")" +
								" VALUES (?, ?, ?, ?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertToken",
				this.session.prepare(
						"INSERT INTO \"" + ElementType.Token + "\"" +
								" (\"uid\", \"documentId\", \"paragraphId\", \"sentenceId\", \"previousTokenId\", \"value\", \"begin\", \"end\")" +
								" VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertLemma",
				this.session.prepare(
						"INSERT INTO \"" + ElementType.Lemma + "\"" +
								" (\"value\")" +
								" VALUES (?);"
				)
		);
		this.preparedStatementMap.put(
				"insertTokenLemmaConnection",
				this.session.prepare(
						"INSERT INTO \"tokenLemmaMap\"" +
								" (\"tokenId\", \"lemmaId\")" +
								" VALUES (?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"insertDocumentLemmaConnection",
				this.session.prepare(
						"INSERT INTO \"documentLemmaMap\"" +
								" (\"documentId\", \"lemmaId\")" +
								" VALUES (?, ?);"
				)
		);
		this.preparedStatementMap.put(
				"findDocumentById",
				this.session.prepare(
						"SELECT * FROM \"" + ElementType.Document + "\"" +
								" WHERE \"uid\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"getDocumentIds",
				this.session.prepare(
						"SELECT \"uid\" FROM \"" + ElementType.Document + "\";"
				)
		);
		this.preparedStatementMap.put(
				"getLemmataForDocument",
				this.session.prepare(
						"SELECT \"lemmaId\"" +
								" FROM \"documentLemmaMap\"" +
								" WHERE \"documentId\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"selectDocument",
				this.session.prepare(
						"SELECT \"language\", \"text\"" +
								" FROM \"" + ElementType.Document + "\"" +
								" WHERE \"uid\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"selectToken",
				this.session.prepare(
						"SELECT \"begin\", \"end\", \"value\"" +
								" FROM \"" + ElementType.Token + "\"" +
								" WHERE \"documentId\" = ?;"
				)
		);
		this.preparedStatementMap.put(
				"countDocumentsContainingLemma",
				this.session.prepare(
						"SELECT COUNT(*)" +
								" FROM \"documentLemmaMap\"" +
								" WHERE \"lemmaId\" = ? ALLOW FILTERING;"
				)
		);
		this.preparedStatementMap.put(
				"selectLemmaDocumentConnections",
				this.session.prepare(
						"SELECT \"lemmaId\", \"documentId\"" +
								" FROM \"documentLemmaMap\";"
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

		session.execute("DROP TABLE IF EXISTS \"" + ElementType.Document + "\"");
		session.execute("DROP TABLE IF EXISTS \"" + ElementType.Paragraph + "\"");
		session.execute("DROP TABLE IF EXISTS \"" + ElementType.Sentence + "\"");
		session.execute("DROP TABLE IF EXISTS \"" + ElementType.Token + "\"");
		session.execute("DROP TABLE IF EXISTS \"" + ElementType.Lemma + "\"");
		session.execute("DROP TABLE IF EXISTS \"tokenLemmaMap\"");
		session.execute("DROP TABLE IF EXISTS \"documentLemmaMap\"");

		session.execute("CREATE TABLE \"" + ElementType.Document + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"text\" VARCHAR, " +
				"  \"language\" VARCHAR, " +
				")");

		session.execute("CREATE TABLE \"" + ElementType.Paragraph + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousParagraphId\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")");
		session.execute("CREATE INDEX ON \"" + ElementType.Paragraph + "\" (\"documentId\")");
		session.execute("CREATE INDEX ON \"" + ElementType.Paragraph + "\" (\"previousParagraphId\");");

		session.execute("CREATE TABLE \"" + ElementType.Sentence + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"paragraphId\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousSentenceId\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")");
		session.execute("CREATE INDEX ON \"" + ElementType.Sentence + "\" (\"paragraphId\")");
		session.execute("CREATE INDEX ON \"" + ElementType.Sentence + "\" (\"documentId\")");
		session.execute("CREATE INDEX ON \"" + ElementType.Sentence + "\" (\"previousSentenceId\");");

		session.execute("CREATE TABLE \"" + ElementType.Token + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"sentenceId\" VARCHAR, " +
				"  \"paragraphId\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousTokenId\" VARCHAR, " +
				"  \"value\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")");
		session.execute("CREATE INDEX ON \"" + ElementType.Token + "\" (\"sentenceId\")");
		session.execute("CREATE INDEX ON \"" + ElementType.Token + "\" (\"paragraphId\")");
		session.execute("CREATE INDEX ON \"" + ElementType.Token + "\" (\"documentId\")");
		session.execute("CREATE INDEX ON \"" + ElementType.Token + "\" (\"previousTokenId\")");
		session.execute("CREATE INDEX ON \"" + ElementType.Token + "\" (\"value\");");

		session.execute("CREATE TABLE \"" + ElementType.Lemma + "\" ( " +
				"  \"value\" VARCHAR primary key " +
				")");

		session.execute("CREATE TABLE \"tokenLemmaMap\" ( " +
				"  \"tokenId\" VARCHAR, " +
				"  \"lemmaId\" VARCHAR, " +
				"  PRIMARY KEY (\"tokenId\", \"lemmaId\") " +
				")");

		session.execute("CREATE TABLE \"documentLemmaMap\" ( " +
				"  \"documentId\" VARCHAR, " +
				"  \"lemmaId\" VARCHAR, " +
				"  PRIMARY KEY (\"documentId\", \"lemmaId\") " +
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

		PreparedStatement aStatement = this.preparedStatementMap.get("createDocument");
		BoundStatement boundStatement = aStatement.bind()
				.setString(0, documentId)
				.setString(1, document.getDocumentText())
				.setString(2, document.getDocumentLanguage());

		session.execute(boundStatement);

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

		PreparedStatement aStatement = this.preparedStatementMap.get("insertParagraph");
		BoundStatement boundStatement = aStatement.bind()
				.setString(0, paragraphId)
				.setString(1, documentId);

		if (previousParagraphId == null)
		{
			boundStatement.setToNull(2);
		} else
		{
			boundStatement.setString(2, previousParagraphId);
		}

		boundStatement.setInt(3, paragraph.getBegin())
				.setInt(4, paragraph.getEnd());

		session.execute(boundStatement);

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
		PreparedStatement aStatement = this.preparedStatementMap.get("insertSentence");
		BoundStatement boundStatement = aStatement.bind()
				.setString(0, sentenceId)
				.setString(1, documentId)
				.setString(2, paragraphId);

		if (previousSentenceId == null)
		{
			boundStatement.setToNull(3);
		} else
		{
			boundStatement.setString(3, previousSentenceId);
		}

		boundStatement.setInt(4, sentence.getBegin());
		boundStatement.setInt(5, sentence.getEnd());

		session.execute(boundStatement);

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
		PreparedStatement aStatement = this.preparedStatementMap.get("insertToken");
		BoundStatement boundStatement = aStatement.bind()
				.setString(0, tokenId)
				.setString(1, documentId)
				.setString(2, paragraphId)
				.setString(3, sentenceId);

		if (previousTokenId == null)
		{
			boundStatement.setToNull(4);
		} else
		{
			boundStatement.setString(4, previousTokenId);
		}

		boundStatement.setString(5, token.getCoveredText());
		boundStatement.setInt(6, token.getBegin());
		boundStatement.setInt(7, token.getEnd());

		session.execute(boundStatement);

		// Get Lemma ID (and insert, if necessary)
		String lemmaId = this.getLemmaId(token.getLemma().getValue());
		// Insert connection from Token to Lemma.
		aStatement = this.preparedStatementMap.get("insertTokenLemmaConnection");
		boundStatement = aStatement.bind()
				.setString(0, tokenId)
				.setString(1, lemmaId);

		session.execute(boundStatement);

		// Insert connection between Document and Lemma, if non exists yet.
		this.insertDocumentLemmaConnection(documentId, lemmaId);

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
		PreparedStatement aStatement = this.preparedStatementMap.get("insertLemma");
		BoundStatement boundStatement = aStatement.bind(value);

		session.execute(boundStatement);

		return value;
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
		PreparedStatement aStatement = this.preparedStatementMap.get("insertDocumentLemmaConnection");
		BoundStatement boundStatement = aStatement.bind(documentId, lemmaId);

		session.execute(boundStatement);
	}

	@Override
	public void checkIfDocumentExists(String documentId) throws DocumentNotFoundException
	{
		PreparedStatement aStatement = this.preparedStatementMap.get("findDocumentById");
		BoundStatement boundStatement = aStatement.bind(documentId);

		ResultSet result = session.execute(boundStatement);

		if (result.one() == null)
		{
			throw new DocumentNotFoundException();
		}
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		List<String> documentIds = new ArrayList<>();

		PreparedStatement aStatement = this.preparedStatementMap.get("getDocumentIds");
		BoundStatement boundStatement = aStatement.bind();

		ResultSet result = this.session.execute(boundStatement);

		for (Row row : result)
		{
			documentIds.add(row.getString(0));
		}

		return documentIds;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
	{
		Set<String> lemmata = new TreeSet<>();
		PreparedStatement aStatement = this.preparedStatementMap.get("getLemmataForDocument");
		BoundStatement boundStatement = aStatement.bind()
				.setString(0, documentId);

		ResultSet result = session.execute(boundStatement);

		for (Row row : result)
		{
			lemmata.add(row.getString(0));
		}

		return lemmata;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{

		try
		{
			// Retrieve Document
			BoundStatement boundSelectDocumentStatement = this
					.preparedStatementMap.get("selectDocument").bind()
					.setString(0, documentId);
			ResultSet documentResult = session
					.execute(boundSelectDocumentStatement);

			Row document = documentResult.one();
			if (document == null)
			{
				throw new DocumentNotFoundException();
			}

			// Create Document CAS
			DocumentMetaData meta = DocumentMetaData.create(aCAS);
			meta.setDocumentId(documentId);
			aCAS.setDocumentLanguage(document.getString(0));
			aCAS.setDocumentText(document.getString(1));

			// Retrieve connected Token
			BoundStatement selectTokenStatement = this
					.preparedStatementMap.get("selectToken").bind();
			selectTokenStatement.setString(0, documentId);
			ResultSet tokenResult = session.execute(selectTokenStatement);

			for (Row row : tokenResult)
			{
				Token xmiToken = new Token(
						aCAS.getJCas(),
						row.getInt(0),
						row.getInt(1)
				);

				Lemma lemma = new Lemma(
						aCAS.getJCas(),
						xmiToken.getBegin(),
						xmiToken.getEnd()
				);
				lemma.setValue(row.getString(2));
				lemma.addToIndexes();
				xmiToken.setLemma(lemma);

				POS pos = new POS(
						aCAS.getJCas(),
						xmiToken.getBegin(),
						xmiToken.getEnd()
				);
				pos.setPosValue(row.getString(2));
				pos.addToIndexes();
				xmiToken.setPos(pos);

				xmiToken.addToIndexes();
			}
		} catch (CASException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		BoundStatement aStatement = this
				.preparedStatementMap.get("countDocumentsContainingLemma").bind()
				.setString(0, lemma);

		ResultSet result = session.execute(aStatement);

		// Always returns a row with one value.
		return (int) result.one().getLong(0);
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		if (type == ElementType.Pos)
		{
			// Counting POSs is the same as counting Tokens, so count Tokens.
			type = ElementType.Token;
		}

		String query = "SELECT COUNT(*) FROM \"" + type + "\";";

		ResultSet result = this.session.execute(query);

		// Always returns a row with one value.
		return (int) result.one().getLong(0);
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		if (type == ElementType.Document)
		{
			return 1;
		}

		String tableName = type.toString();
		if (type == ElementType.Pos)
		{
			// Counting POSs is the same as counting Tokens, so count Tokens.
			tableName = ElementType.Token.toString();
		}
		if (type == ElementType.Lemma)
		{
			tableName = "documentLemmaMap";
		}

		// I don't cant to prepare the statement for each tableName beforehand,
		// so they are prepared and stored when needed.
		BoundStatement aStatement = this.getOrPrepare(
				"countElementsInDocumentOfType" + tableName,
				"SELECT COUNT(*) FROM \"" + tableName + "\"" +
						" WHERE \"documentId\" = ?;"
		).bind()
				.setString(0, documentId);

		ResultSet result = session.execute(aStatement);

		// Always returns a row with one value.
		return (int) result.one().getLong(0);
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		// => type is either Token, Lemma or POS.

		if (type == ElementType.Pos)
		{
			// Pos are stored inside Tokens.
			type = ElementType.Token;
		}

		BoundStatement aStatement = this.getOrPrepare(
				"countElementOfType" + type + "WithValue",
				"SELECT COUNT(*)" +
						" FROM \"" + type + "\"" +
						" WHERE \"value\" = ?;"
		).bind()
				.setString(0, value);

		ResultSet result = this.session.execute(aStatement);

		// Always returns a row with one value.
		return (int) result.one().getLong(0);
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		this.checkIfDocumentExists(documentId);
		// => type is either Token, Lemma or POS.

		String query;
		if (type == ElementType.Token || type == ElementType.Pos)
		{
			query = "SELECT COUNT(*)" +
					" FROM \"" + ElementType.Token + "\"" +
					" WHERE \"documentId\" = ? AND \"value\" = ? ALLOW FILTERING;";
		} else
		{ // => type == ElementType.Lemma
			query = "SELECT COUNT(*)" +
					" FROM \"documentLemmaMap\"" +
					" WHERE \"documentId\" = ? " +
					"   AND \"lemmaId\" = ?;";
		}

		BoundStatement aStatement = this.getOrPrepare(
				"countElementsInDocumentOfType" + type.toString() + "WithValue",
				query
		).bind()
				.setString(0, documentId)
				.setString(1, value);

		ResultSet result = this.session.execute(aStatement);

		// Always returns a row with one value.
		return (int) result.one().getLong(0);
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		Map<String, String> lemmaDocumentMap = new HashMap<>();
		Map<String, Integer> lemmaOccurenceMap = new HashMap<>();
		BoundStatement aStatement = this
				.preparedStatementMap.get("selectLemmaDocumentConnections")
				.bind();
		ResultSet result = this.session.execute(aStatement);

		for (Row row : result)
		{
			lemmaDocumentMap.put(
					row.getString(0), // LemmaId
					row.getString(1) // DocumntId
			);
		}

		// Cassandra doesn't support group by, so we'll have to do it ourselves.
		lemmaDocumentMap.entrySet()
				.parallelStream()
				.collect(Collectors.groupingByConcurrent(Map.Entry::getKey))
				.entrySet()
				.parallelStream()
				.forEach(entry -> lemmaOccurenceMap.put(
						entry.getKey(),
						entry.getValue().size()
				));

		return lemmaOccurenceMap;
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
		return null;
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		return null;
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
