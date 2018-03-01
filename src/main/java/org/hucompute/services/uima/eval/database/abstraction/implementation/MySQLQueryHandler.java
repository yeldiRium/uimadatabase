package org.hucompute.services.uima.eval.database.abstraction.implementation;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.neo4j.driver.v1.Session;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MySQLQueryHandler extends AbstractQueryHandler
{
	protected Connection connection;

	public MySQLQueryHandler(Connection connection)
	{
		this.connection = connection;
	}

	@Override
	public void setUpDatabase()
	{
		try (Statement aStatement = this.connection.createStatement())
		{
			String dropTables = "DROP TABLE IF EXISTS documentLemmaMap, " +
					"tokenLemmaMap, " + ElementType.Pos + ", " +
					ElementType.Lemma + ", " + ElementType.Token + ", " +
					ElementType.Sentence + ", " + ElementType.Paragraph + ", " +
					ElementType.Document + ";";
			String createDocumentTable = "CREATE TABLE " + ElementType.Document + " ( " +
					"  id VARCHAR(50) NOT NULL, " +
					"  text MEDIUMTEXT, " +
					"  language VARCHAR(50), " +
					"  PRIMARY KEY (id) " +
					")";
			String createParagraphTable = "CREATE TABLE " + ElementType.Paragraph + " ( " +
					"  id BIGINT NOT NULL AUTO_INCREMENT, " +
					"  documentId VARCHAR(50) NOT NULL, " +
					"  previousParagraphId BIGINT NOT NULL, " +
					"  begin INT NOT NULL, " +
					"  end INT NOT NULL, " +
					"  PRIMARY KEY (id), " +
					"  FOREIGN KEY (documentId) REFERENCES " + ElementType.Document + "(id), " +
					"  FOREIGN KEY (previousParagraphId) REFERENCES " + ElementType.Paragraph + "(id) " +
					")";
			String createSentenceTable = "CREATE TABLE " + ElementType.Sentence + " ( " +
					"  id BIGINT NOT NULL AUTO_INCREMENT, " +
					"  paragraphId BIGINT NOT NULL, " +
					"  documentId VARCHAR(50) NOT NULL, " +
					"  previousSentenceId BIGINT NOT NULL, " +
					"  begin INT NOT NULL, " +
					"  end INT NOT NULL, " +
					"  PRIMARY KEY (id), " +
					"  FOREIGN KEY (paragraphId) REFERENCES " + ElementType.Paragraph + "(id), " +
					"  FOREIGN KEY (documentId) REFERENCES " + ElementType.Document + "(id), " +
					"  FOREIGN KEY (previousSentenceId) REFERENCES " + ElementType.Sentence + "(id) " +
					")";
			String createTokenTable = "CREATE TABLE " + ElementType.Token + " ( " +
					"  id BIGINT NOT NULL AUTO_INCREMENT, " +
					"  sentenceId BIGINT NOT NULL, " +
					"  paragraphId BIGINT NOT NULL, " +
					"  documentId VARCHAR(50) NOT NULL, " +
					"  previousTokenId BIGINT NOT NULL, " +
					"  value VARCHAR(255) NOT NULL, " +
					"  begin INT NOT NULL, " +
					"  end INT NOT NULL, " +
					"  PRIMARY KEY (id), " +
					"  FOREIGN KEY (sentenceId) REFERENCES " + ElementType.Sentence + "(id), " +
					"  FOREIGN KEY (paragraphId) REFERENCES " + ElementType.Paragraph + "(id), " +
					"  FOREIGN KEY (documentId) REFERENCES " + ElementType.Document + "(id), " +
					"  FOREIGN KEY (previousTokenId) REFERENCES " + ElementType.Token + "(id) " +
					")";
			String createLemmaTable = "CREATE TABLE " + ElementType.Lemma + " ( " +
					"  id BIGINT NOT NULL AUTO_INCREMENT, " +
					"  value VARCHAR(255) NOT NULL, " +
					"  PRIMARY KEY (id), " +
					"  UNIQUE(value) " +
					")";
			String createPosTable = "CREATE TABLE " + ElementType.Pos + " ( " +
					"  tokenId BIGINT NOT NULL, " +
					"  begin INT NOT NULL, " +
					"  end INT NOT NULL, " +
					"  FOREIGN KEY (tokenId) REFERENCES " + ElementType.Token + "(id) " +
					")";

			String createTokenLemmaMap = "CREATE TABLE tokenLemmaMap ( " +
					"  tokenId BIGINT NOT NULL, " +
					"  lemmaId BIGINT NOT NULL, " +
					"  FOREIGN KEY (tokenId) REFERENCES " + ElementType.Token + "(id), " +
					"  FOREIGN KEY (lemmaId) REFERENCES " + ElementType.Lemma + "(id) " +
					")";
			String createDocumentLemmaMap = "CREATE TABLE documentLemmaMap ( " +
					"  documentId VARCHAR(50) NOT NULL, " +
					"  lemmaId BIGINT NOT NULL, " +
					"  FOREIGN KEY (documentId) REFERENCES " + ElementType.Document + "(id), " +
					"  FOREIGN KEY (lemmaId) REFERENCES " + ElementType.Lemma + "(id) " +
					")";

			aStatement.executeUpdate(dropTables);
			aStatement.executeUpdate(createDocumentTable);
			aStatement.executeUpdate(createParagraphTable);
			aStatement.executeUpdate(createSentenceTable);
			aStatement.executeUpdate(createTokenTable);
			aStatement.executeUpdate(createLemmaTable);
			aStatement.executeUpdate(createPosTable);
			aStatement.executeUpdate(createTokenLemmaMap);
			aStatement.executeUpdate(createDocumentLemmaMap);
			} catch (SQLException e)
		{
			throw new QHException(e);
		}
	}

	/**
	 * Delete Tokens, Sentences and Paragraphs in reversed order to not run into
	 * problems with the self-referencing foreign key.
	 */
	@Override
	public void clearDatabase()
	{
		try (Statement aStatement = this.connection.createStatement())
		{
			String clearTokenLemmaMap = "DELETE FROM tokenLemmaMap;";
			String clearDocumentLemmaMap = "DELETE FROM documentLemmaMap;";

			String clearLemmata = "DELETE FROM " + ElementType.Lemma + ";";
			String clearPos = "DELETE FROM " + ElementType.Pos + ";";
			String clearTokens = "DELETE FROM " + ElementType.Token + " ORDER BY id DESC;";
			String clearSentences = "DELETE FROM " + ElementType.Sentence + " ORDER BY id DESC;";
			String clearParagraphs = "DELETE FROM " + ElementType.Paragraph + " ORDER BY id DESC;";
			String clearDocuments = "DELETE FROM " + ElementType.Document + ";";

			aStatement.executeUpdate(clearTokenLemmaMap);
			aStatement.executeUpdate(clearDocumentLemmaMap);
			aStatement.executeUpdate(clearLemmata);
			aStatement.executeUpdate(clearPos);
			aStatement.executeUpdate(clearTokens);
			aStatement.executeUpdate(clearSentences);
			aStatement.executeUpdate(clearParagraphs);
			aStatement.executeUpdate(clearDocuments);
		} catch (SQLException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public void storeJCasDocument(JCas document)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		String createDocument = "INSERT INTO " + ElementType.Document + " " +
				"VALUES ( ?, ?, ?);";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(createDocument))
		{
			aStatement.setString(1, documentId);
			aStatement.setString(2, document.getDocumentText());
			aStatement.setString(3, document.getDocumentLanguage());
			aStatement.executeUpdate();
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public void storeParagraph(
			Paragraph paragraph, JCas document, Paragraph previousParagraph
	)
	{

	}

	@Override
	public void storeParagraph(Paragraph paragraph, JCas document)
	{

	}

	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph,
			Sentence previousSentence
	)
	{

	}

	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph
	)
	{

	}

	@Override
	public void storeToken(
			Token token,
			JCas document,
			Paragraph paragraph,
			Sentence sentence,
			Token previousToken
	)
	{

	}

	@Override
	public void storeToken(
			Token token, JCas document, Paragraph paragraph, Sentence sentence
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
