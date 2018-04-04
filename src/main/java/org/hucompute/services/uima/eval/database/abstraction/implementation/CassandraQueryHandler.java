package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.datastax.driver.core.*;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.connection.Connections;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class CassandraQueryHandler extends AbstractQueryHandler
{
	protected Session session;

	public CassandraQueryHandler(Session session)
	{
		this.session = session;
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.Cassandra;
	}

	@Override
	public void setUpDatabase()
	{
		String query = "CREATE KEYSPACE IF NOT EXISTS " +
				System.getenv("CASSANDRA_DB") + " WITH replication = {" +
				"'class':'SimpleStrategy'," +
				"'replication_factor':1" +
				"};";
		session.execute(query);

		session.execute("USE " + System.getenv("CASSANDRA_DB"));

		query = "DROP TABLE IF EXISTS \"" + ElementType.Document + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"" + ElementType.Paragraph + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"" + ElementType.Sentence + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"" + ElementType.Token + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"" + ElementType.Lemma + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"tokenLemmaMap\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"documentLemmaMap\"";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Document + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"text\" VARCHAR, " +
				"  \"language\" VARCHAR, " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Paragraph + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousParagraphId\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Sentence + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"paragraphId\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousSentenceId\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Token + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"sentenceId\" VARCHAR, " +
				"  \"paragraphId\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousTokenId\" VARCHAR, " +
				"  \"value\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Lemma + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"value\" VARCHAR, " +
				")";
		session.execute(query);


		query = "CREATE TABLE \"tokenLemmaMap\" ( " +
				"  \"uid\" uuid primary key, " +
				"  \"tokenId\" VARCHAR, " +
				"  \"lemmaId\" VARCHAR " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"documentLemmaMap\" ( " +
				"  \"uid\" uuid primary key, " +
				"  \"documentId\" VARCHAR, " +
				"  \"lemmaId\" VARCHAR " +
				")";
		session.execute(query);
	}

	@Override
	public void openDatabase() throws IOException
	{
		session.execute("USE " + System.getenv("CASSANDRA_DB"));
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
		String createDocument = "INSERT INTO \"" + ElementType.Document + "\"" +
				" (\"uid\", \"text\", \"language\")" +
				" VALUES (?, ?, ?);";
		PreparedStatement aStatement = this.session.prepare(createDocument);
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

		String insertParagraph = "INSERT INTO \"" + ElementType.Paragraph + "\"" +
				" (\"uid\", \"documentId\", \"previousParagraphId\", \"begin\", \"end\")" +
				" VALUES (?, ?, ?, ?, ?);";
		PreparedStatement aStatement = this.session.prepare(insertParagraph);
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
		String insertSentence = "INSERT INTO \"" + ElementType.Sentence + "\"" +
				" (\"uid\", \"documentId\", \"paragraphId\", \"previousSentenceId\", \"begin\", \"end\")" +
				" VALUES (?, ?, ?, ?, ?, ?);";
		PreparedStatement aStatement = this.session.prepare(insertSentence);
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
		String insertSentence = "INSERT INTO \"" + ElementType.Token + "\"" +
				" (\"uid\", \"documentId\", \"paragraphId\", \"sentenceId\", \"previousTokenId\", \"value\", \"begin\", \"end\")" +
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
		PreparedStatement aStatement = this.session.prepare(insertSentence);
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
		String insertTokenLemmaConnection = "INSERT INTO \"tokenLemmaMap\"" +
				" (\"tokenId\", \"lemmaId\")" +
				" VALUES (?, ?);";
		aStatement = this.session.prepare(insertTokenLemmaConnection);
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
	 * Otherwise retrieves the existing one.
	 *
	 * @param value The Lemma's value.
	 * @return The Lemma's id.
	 */
	protected String getLemmaId(String value)
	{
		String selectLemma = "SELECT \"uid\" FROM \"" + ElementType.Lemma + "\"" +
				" WHERE \"value\" = ?";
		PreparedStatement aStatement = this.session.prepare(selectLemma);
		BoundStatement boundStatement = aStatement.bind(value);
		ResultSet result = session.execute(boundStatement);

		Row lemma = result.one();
		if (lemma != null)
		{
			return lemma.getString(1);
		}

		// If no Lemma was found, a new one has to be created.
		String lemmaId = UUID.randomUUID().toString();
		String insertLemma = "INSERT INTO \"" + ElementType.Lemma + "\"" +
				" (\"uid\", \"value\")" +
				" VALUES (?, ?);";
		aStatement = this.session.prepare(insertLemma);
		boundStatement = aStatement.bind(lemmaId, value);

		session.execute(boundStatement);

		return lemmaId;
	}

	/**
	 * Inserts a connection from Document to Lemma into the table
	 * documentLemmaMap.
	 *
	 * @param documentId
	 * @param lemmaId
	 * @return False, if the connection already existed. True otherwise.
	 */
	protected boolean insertDocumentLemmaConnection(
			String documentId, String lemmaId
	)
	{
		String selectConnection = "SELECT `documentId`, `lemmaId` " +
				" FROM documentLemmaMap" +
				" WHERE `documentId` = ? AND `lemmaId` = ?;";
		PreparedStatement aStatement = this.session.prepare(selectConnection);
		BoundStatement boundStatement = aStatement.bind(documentId, lemmaId);

		ResultSet result = session.execute(boundStatement);

		Row documentLemmaConnection = result.one();
		if (documentLemmaConnection != null)
		{
			// A Connection exists, nothing to be done here.
			return false;
		}

		String insertConnection = "INSERT INTO documentLemmaMap" +
				" (\"documentId\", \"lemmaId\")" +
				" VALUES (?, ?);";
		aStatement = this.session.prepare(insertConnection);
		boundStatement = aStatement.bind(documentId, lemmaId);

		session.execute(boundStatement);

		return true;
	}

	@Override
	public void checkIfDocumentExists(String documentId) throws DocumentNotFoundException
	{
		String query = "SELECT * FROM " + ElementType.Document +
				" WHERE \"id\" = ?;";
		PreparedStatement aStatement = this.session.prepare(query);
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
		return null;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
	{
		return null;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{

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
		return 0;
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException
	{
		return 0;
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		return null;
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
