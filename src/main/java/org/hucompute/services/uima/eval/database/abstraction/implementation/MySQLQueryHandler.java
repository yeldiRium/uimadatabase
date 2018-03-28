package org.hucompute.services.uima.eval.database.abstraction.implementation;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.lang.StringUtils;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.connection.Connections;

import java.sql.*;
import java.util.*;

public class MySQLQueryHandler extends AbstractQueryHandler
{
	protected Connection connection;

	public MySQLQueryHandler(Connection connection)
	{
		this.connection = connection;
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.MySQL;
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
					"  id VARCHAR(36) NOT NULL, " +
					"  documentId VARCHAR(50) NOT NULL, " +
					"  previousParagraphId VARCHAR(36), " +
					"  begin INT NOT NULL, " +
					"  end INT NOT NULL, " +
					"  PRIMARY KEY (id), " +
					"  FOREIGN KEY (documentId) REFERENCES " + ElementType.Document + "(id), " +
					"  FOREIGN KEY (previousParagraphId) REFERENCES " + ElementType.Paragraph + "(id) " +
					")";
			String createSentenceTable = "CREATE TABLE " + ElementType.Sentence + " ( " +
					"  id VARCHAR(36) NOT NULL, " +
					"  paragraphId VARCHAR(36) NOT NULL, " +
					"  documentId VARCHAR(50) NOT NULL, " +
					"  previousSentenceId VARCHAR(36), " +
					"  begin INT NOT NULL, " +
					"  end INT NOT NULL, " +
					"  PRIMARY KEY (id), " +
					"  FOREIGN KEY (paragraphId) REFERENCES " + ElementType.Paragraph + "(id), " +
					"  FOREIGN KEY (documentId) REFERENCES " + ElementType.Document + "(id), " +
					"  FOREIGN KEY (previousSentenceId) REFERENCES " + ElementType.Sentence + "(id) " +
					")";
			String createTokenTable = "CREATE TABLE " + ElementType.Token + " ( " +
					"  id VARCHAR(36) NOT NULL, " +
					"  sentenceId VARCHAR(36) NOT NULL, " +
					"  paragraphId VARCHAR(36) NOT NULL, " +
					"  documentId VARCHAR(50) NOT NULL, " +
					"  previousTokenId VARCHAR(36), " +
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
					"  id VARCHAR(36) NOT NULL, " +
					"  value VARCHAR(255) NOT NULL, " +
					"  PRIMARY KEY (id), " +
					"  UNIQUE(value) " +
					")";

			String createTokenLemmaMap = "CREATE TABLE tokenLemmaMap ( " +
					"  tokenId VARCHAR(36) NOT NULL, " +
					"  lemmaId VARCHAR(36) NOT NULL, " +
					"  FOREIGN KEY (tokenId) REFERENCES " + ElementType.Token + "(id), " +
					"  FOREIGN KEY (lemmaId) REFERENCES " + ElementType.Lemma + "(id) " +
					")";
			String createDocumentLemmaMap = "CREATE TABLE documentLemmaMap ( " +
					"  documentId VARCHAR(50) NOT NULL, " +
					"  lemmaId VARCHAR(36) NOT NULL, " +
					"  FOREIGN KEY (documentId) REFERENCES " + ElementType.Document + "(id), " +
					"  FOREIGN KEY (lemmaId) REFERENCES " + ElementType.Lemma + "(id) " +
					")";

			aStatement.executeUpdate(dropTables);
			aStatement.executeUpdate(createDocumentTable);
			aStatement.executeUpdate(createParagraphTable);
			aStatement.executeUpdate(createSentenceTable);
			aStatement.executeUpdate(createTokenTable);
			aStatement.executeUpdate(createLemmaTable);
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
			String clearTokens = "DELETE FROM " + ElementType.Token + " ORDER BY id DESC;";
			String clearSentences = "DELETE FROM " + ElementType.Sentence + " ORDER BY id DESC;";
			String clearParagraphs = "DELETE FROM " + ElementType.Paragraph + " ORDER BY id DESC;";
			String clearDocuments = "DELETE FROM " + ElementType.Document + ";";

			aStatement.executeUpdate(clearTokenLemmaMap);
			aStatement.executeUpdate(clearDocumentLemmaMap);
			aStatement.executeUpdate(clearLemmata);
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
	public String storeJCasDocument(JCas document)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		String createDocument = "INSERT INTO " + ElementType.Document +
				" (`id`, `text`, `language`)" +
				" VALUES (?, ?, ?);";
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

		String insertParagraph = "INSERT INTO " + ElementType.Paragraph +
				" (`id`, `documentId`, `previousParagraphId`, `begin`, `end`)" +
				" VALUES (?, ?, ?, ?, ?);";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(insertParagraph))
		{
			aStatement.setString(1, paragraphId);
			aStatement.setString(2, documentId);

			if (previousParagraphId == null)
			{
				aStatement.setNull(3, Types.VARCHAR);
			} else
			{
				aStatement.setString(3, previousParagraphId);
			}

			aStatement.setInt(4, paragraph.getBegin());
			aStatement.setInt(5, paragraph.getEnd());

			aStatement.executeUpdate();
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

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
		String insertSentence = "INSERT INTO " + ElementType.Sentence +
				" (`id`, `documentId`, `paragraphId`, `previousSentenceId`, `begin`, `end`) " +
				" VALUES (?, ?, ?, ?, ?, ?);";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(insertSentence))
		{
			aStatement.setString(1, sentenceId);
			aStatement.setString(2, documentId);
			aStatement.setString(3, paragraphId);

			if (previousSentenceId == null)
			{
				aStatement.setNull(4, Types.VARCHAR);
			} else
			{
				aStatement.setString(4, previousSentenceId);
			}

			aStatement.setInt(5, sentence.getBegin());
			aStatement.setInt(6, sentence.getEnd());

			aStatement.executeUpdate();
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

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
		String insertSentence = "INSERT INTO " + ElementType.Token +
				" (`id`, `documentId`, `paragraphId`, `sentenceId`, `previousTokenId`, `value`, `begin`, `end`)" +
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(insertSentence))
		{
			aStatement.setString(1, tokenId);
			aStatement.setString(2, documentId);
			aStatement.setString(3, paragraphId);
			aStatement.setString(4, sentenceId);

			if (previousTokenId == null)
			{
				aStatement.setNull(5, Types.VARCHAR);
			} else
			{
				aStatement.setString(5, previousTokenId);
			}

			aStatement.setString(6, token.getCoveredText());
			aStatement.setInt(7, token.getBegin());
			aStatement.setInt(8, token.getEnd());

			aStatement.executeUpdate();
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		// Get Lemma ID (and insert, if necessary)
		String lemmaId = this.getLemmaId(token.getLemma().getValue());
		// Insert connection from Token to Lemma.
		String insertTokenLemmaConnection = "INSERT INTO `tokenLemmaMap`" +
				" (`tokenId`, `lemmaId`)" +
				" VALUES (?, ?);";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(
						     insertTokenLemmaConnection
				     ))
		{
			aStatement.setString(1, tokenId);
			aStatement.setString(2, lemmaId);

			aStatement.executeUpdate();
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

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
		String selectLemma = "SELECT `id` FROM " + ElementType.Lemma +
				" WHERE `value` = ?";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(selectLemma))
		{
			aStatement.setString(1, value);
			ResultSet result = aStatement.executeQuery();

			if (result.next())
			{
				// If a Lemma was found, return its id.
				return result.getString(1);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		// If no Lemma was found, a new one has to be created.
		String lemmaId = UUID.randomUUID().toString();
		String insertLemma = "INSERT INTO " + ElementType.Lemma +
				" (`id`, `value`)" +
				" VALUES (?, ?);";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(insertLemma))
		{
			aStatement.setString(1, lemmaId);
			aStatement.setString(2, value);

			aStatement.executeUpdate();
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

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
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(selectConnection))
		{
			aStatement.setString(1, documentId);
			aStatement.setString(2, lemmaId);

			ResultSet result = aStatement.executeQuery();

			if (result.next())
			{
				// A Connection exists, nothing to be done here.
				return false;
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
		}

		String insertConnection = "INSERT INTO documentLemmaMap" +
				" (`documentId`, `lemmaId`)" +
				" VALUES (?, ?);";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(insertConnection))
		{
			aStatement.setString(1, documentId);
			aStatement.setString(2, lemmaId);

			aStatement.executeUpdate();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}

		return true;
	}

	@Override
	public void checkIfDocumentExists(String documentId)
			throws DocumentNotFoundException
	{
		String query = "SELECT * FROM " + ElementType.Document +
				" WHERE `id` = ?;";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, documentId);

			ResultSet result = aStatement.executeQuery();

			if (!result.next())
			{
				throw new DocumentNotFoundException();
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		List<String> documentIds = new ArrayList<>();
		String query = "SELECT `id`" +
				" FROM " + ElementType.Document + ";";
		try (Statement aStatement = this.connection.createStatement())
		{
			ResultSet result = aStatement.executeQuery(query);
			while (result.next())
			{
				documentIds.add(result.getString(1));
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
		return documentIds;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
	{
		Set<String> lemmata = new TreeSet<>();
		String query = "SELECT `lemma`.`value`" +
				" FROM " + ElementType.Lemma + " AS `lemma`" +
				"     JOIN documentLemmaMap AS `dlm`" +
				"         ON `lemma`.`id` = `dlm`.`lemmaId`" +
				" WHERE `dlm`.`documentId` = ?;";
		try (PreparedStatement aStatement = this.connection.prepareStatement(
				query
		))
		{
			aStatement.setString(1, documentId);

			ResultSet lemmataResult = aStatement.executeQuery();

			while (lemmataResult.next())
			{
				lemmata.add(lemmataResult.getString(1));
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
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
			String selectDocumentQuery = "SELECT `language`, `text`" +
					" FROM " + ElementType.Document +
					" WHERE `id` = ?;";
			PreparedStatement selectDocumentStatement =
					this.connection.prepareStatement(selectDocumentQuery);
			selectDocumentStatement.setString(1, documentId);
			ResultSet documentResult = selectDocumentStatement.executeQuery();
			if (!documentResult.next())
			{
				throw new DocumentNotFoundException();
			}

			// Create Document CAS
			DocumentMetaData meta = DocumentMetaData.create(aCAS);
			meta.setDocumentId(documentId);
			aCAS.setDocumentLanguage(documentResult.getString(1));
			aCAS.setDocumentText(documentResult.getString(2));

			// Retrieve connected Tokens
			String selectTokenQuery = "SELECT `begin`, `end`, `value`" +
					" FROM " + ElementType.Token +
					" WHERE `documentId` = ?;";
			PreparedStatement selectTokenStatement =
					this.connection.prepareStatement(selectTokenQuery);
			selectTokenStatement.setString(1, documentId);
			ResultSet tokenResult = selectTokenStatement.executeQuery();

			while (tokenResult.next())
			{
				Token xmiToken = new Token(
						aCAS.getJCas(),
						tokenResult.getInt(1),
						tokenResult.getInt(2)
				);

				Lemma lemma = new Lemma(
						aCAS.getJCas(),
						xmiToken.getBegin(),
						xmiToken.getEnd()
				);
				lemma.setValue(tokenResult.getString(3));
				lemma.addToIndexes();
				xmiToken.setLemma(lemma);

				POS pos = new POS(
						aCAS.getJCas(),
						xmiToken.getBegin(),
						xmiToken.getEnd()
				);
				pos.setPosValue(tokenResult.getString(3));
				pos.addToIndexes();
				xmiToken.setPos(pos);

				xmiToken.addToIndexes();
			}
		} catch (CASException | SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		String query = "SELECT COUNT(*)" +
				" FROM " + ElementType.Lemma + " AS `lemma`" +
				"     JOIN documentLemmaMap AS `dlm`" +
				"         ON `lemma`.`id` = `dlm`.`lemmaId`" +
				" WHERE `lemma`.`value` = ?;";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, lemma);

			ResultSet result = aStatement.executeQuery();

			// Always returns a row with one value.
			result.next();
			return result.getInt(1);
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		if (type == ElementType.Pos)
		{
			// Counting POSs is the same as counting Tokens, so count Tokens.
			type = ElementType.Token;
		}

		String query = "SELECT COUNT(*) FROM " + type + ";";
		try
		{
			Statement aStatement = this.connection.createStatement();

			ResultSet result = aStatement.executeQuery(query);

			// Always returns a row with one value.
			result.next();
			return result.getInt(1);
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	/**
	 * Since all model tables (except Lemma and Document) have a column
	 * "documentId", we can select the table dynamically based on type and count
	 * all rows with the given DocumentId.
	 * <p>
	 * In the case of type Lemma, we can use the table "documentLemmaMap" for
	 * this, since it also has a "documentId" column.
	 *
	 * @param documentId The id of the document from which elements are to be
	 *                   counted.
	 * @param type       Instance of ElementType, namely Token, Lemma, Pos,
	 *                   Document, sentence or Paragraph.
	 * @return An integer.
	 * @throws DocumentNotFoundException If the document doesn't exist.
	 */
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

		String query = "SELECT COUNT(*) FROM " + tableName +
				" WHERE `documentId` = ?;";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, documentId);

			ResultSet result = aStatement.executeQuery();

			// Always returns a row with one value.
			result.next();
			return result.getInt(1);
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
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

		String query = "SELECT COUNT(*)" +
				" FROM " + type +
				" WHERE `value` = ?;";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, value);

			ResultSet result = aStatement.executeQuery();

			// Always returns a row with one value.
			result.next();
			return result.getInt(1);
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
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
					" FROM " + ElementType.Token +
					" WHERE `documentId` = ? AND `value` = ?;";
		} else { // => type == ElementType.Lemma
			query = "SELECT COUNT(*)" +
					" FROM " + ElementType.Lemma + " as `lemma`" +
					"      JOIN documentLemmaMap as `dlm`" +
					"          ON `lemma`.`id` = `dlm`.`lemmaId`" +
					" WHERE `dlm`.`documentId` = ? " +
					"   AND `lemma`.`value` = ?;";
		}

		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, documentId);
			aStatement.setString(2, value);

			ResultSet result = aStatement.executeQuery();

			// Always returns a row with one value.
			result.next();
			return result.getInt(1);
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	/**
	 * Since every Token is connected to exactly one Document (the one it is
	 * contained in), we do not need to query the Documents and can instead
	 * count in how many Tokens a Lemma occurs.
	 * <p>
	 * Since we have the connection table "tokenLemmaMap", we can count how
	 * many connections each Lemma has.
	 * <p>
	 * This can only be a problem, if there are dangling Tokens (which do not
	 * have a connection to a Document). However, this should never happen.
	 *
	 * @return The amount of occurences of each Lemma an all Documents.
	 */
	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		Map<String, Integer> lemmaOccurenceMap = new HashMap<>();
		String query = "SELECT `lemma`.`value`, COUNT(*)" +
				" FROM " + ElementType.Lemma + " AS `lemma`" +
				"     JOIN tokenLemmaMap AS `tlm`" +
				"         ON `lemma`.`id` = `tlm`.`lemmaId`" +
				" GROUP BY `lemma`.`id`;";
		try (Statement aStatement = this.connection.createStatement())
		{
			ResultSet result = aStatement.executeQuery(query);

			while (result.next())
			{
				lemmaOccurenceMap.put(
						result.getString(1),
						result.getInt(2)
				);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return lemmaOccurenceMap;
	}

	/**
	 * For each Document join the Tokens and the tokenLemmaMap.
	 * Then the amount of rows for each Document is the amount of Lemma
	 * occurences. To get the amount of different Token values, they have to be
	 * counted distinctly.
	 *
	 * @return Map(String DocumentId, Double TTR - value)
	 */
	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		Map<String, Double> ttrMap = new HashMap<>();
		String query = "SELECT `document`.`id`," +
				"     (COUNT(*)/COUNT(DISTINCT `token`.`value`))" +
				" FROM " + ElementType.Document + " AS `document`" +
				"     JOIN " + ElementType.Token + " AS `token`" +
				"         ON `document`.`id` = `token`.`documentId`" +
				"         JOIN tokenLemmaMap AS `tlm`" +
				"             ON `token`.`id` = `tlm`.`tokenId`" +
				" GROUP BY `document`.`id`;";
		try (Statement aStatement = this.connection.createStatement())
		{
			ResultSet result = aStatement.executeQuery(query);

			while (result.next())
			{
				ttrMap.put(result.getString(1), result.getDouble(2));
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return ttrMap;
	}

	/**
	 * See #calculateTTRForAllDocuments for SQL explanation.
	 *
	 * @param documentId The id of the document to get the TTR from.
	 * @return Map(String DocumentId, Double TTR - value)
	 * @throws DocumentNotFoundException
	 */
	@Override
	public Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		return this.calculateTTRForCollectionOfDocuments(
				Arrays.asList(documentId)
		).get(documentId);
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(
			Collection<String> documentIds
	)
	{
		Map<String, Double> ttrMap = new HashMap<>();

		// Since the java driver does not support preparing statements with
		// arrays of values, we have to generate a fitting amount of prepared
		// statement markers and iterate over the values.
		String questionMarks = "(?" +
				StringUtils.repeat(", ?", documentIds.size() - 1) +
				")";
		String query = "SELECT `document`.`id`," +
				"     (COUNT(*)/COUNT(DISTINCT `token`.`value`))" +
				" FROM " + ElementType.Document + " AS `document`" +
				"     JOIN " + ElementType.Token + " AS `token`" +
				"         ON `document`.`id` = `token`.`documentId`" +
				"         JOIN tokenLemmaMap AS `tlm`" +
				"             ON `token`.`id` = `tlm`.`tokenId`" +
				" WHERE `document`.`id` IN " + questionMarks + "" +
				" GROUP BY `document`.`id`;";

		int counter = 1;
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			// Loop over document ids and set each to a separate prepared value
			// slot.
			for (String documentId : documentIds) {
				aStatement.setString(counter++, documentId);
			}

			ResultSet result = aStatement.executeQuery();

			while (result.next()) {
				ttrMap.put(result.getString(1), result.getDouble(2));
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return ttrMap;
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		Map<String, Integer> lemmaFrequencyMap = new HashMap<>();
		String query = "SELECT `lemma`.`value`, COUNT(*)" +
				" FROM " + ElementType.Lemma + " AS `lemma`" +
				"     JOIN tokenLemmaMap AS `tlm`" +
				"         ON `lemma`.`id` = `tlm`.`lemmaId`" +
				"         JOIN " + ElementType.Token + " AS `token`" +
				"             ON `tlm`.`tokenId` = `token`.`id`" +
				" WHERE `token`.`documentId` = ?" +
				" GROUP BY `lemma`.`id`;";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, documentId);

			ResultSet result = aStatement.executeQuery();

			while (result.next())
			{
				lemmaFrequencyMap.put(
						result.getString(1),
						result.getInt(2)
				);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return lemmaFrequencyMap;
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		String query = "SELECT COUNT(*)" +
				" FROM " + ElementType.Lemma + " AS `lemma`" +
				"     JOIN tokenLemmaMap AS `tlm`" +
				"         ON `lemma`.`id` = `tlm`.`lemmaId`" +
				"         JOIN " + ElementType.Token + " AS `token`" +
				"             ON `tlm`.`tokenId` = `token`.`id`" +
				" WHERE `token`.`documentId` = ? AND `lemma`.`value` = ?;";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, documentId);
			aStatement.setString(2, lemma);

			ResultSet result = aStatement.executeQuery();

			result.next();
			return result.getInt(1);
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		List<String> biGramList = new ArrayList<>();
		String query = "SELECT CONCAT(`a`.`value`, \"-\", `b`.`value`)" +
				" FROM " + ElementType.Token + " AS `a`" +
				"     LEFT JOIN " + ElementType.Token + " AS `b`" +
				"         ON `a`.`id` = `b`.`previousTokenId`" +
				" WHERE `a`.`documentId` = ?;";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, documentId);

			ResultSet result = aStatement.executeQuery();

			while (result.next()) {
				String nextValue = result.getString(1);
				if (nextValue == null) {
					continue;
				}
				biGramList.add(nextValue);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return biGramList;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		List<String> biGramList = new ArrayList<>();
		String query = "SELECT CONCAT(`a`.`value`, \"-\", `b`.`value`)" +
				" FROM " + ElementType.Token + " AS `a`" +
				"     LEFT JOIN " + ElementType.Token + " AS `b`" +
				"         ON `a`.`id` = `b`.`previousTokenId`;";
		try (Statement aStatement = this.connection.createStatement())
		{
			ResultSet result = aStatement.executeQuery(query);

			while (result.next()) {
				String nextValue = result.getString(1);
				if (nextValue == null) {
					continue;
				}
				biGramList.add(nextValue);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return biGramList;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		List<String> biGramList = new ArrayList<>();

		// Since the java driver does not support preparing statements with
		// arrays of values, we have to generate a fitting amount of prepared
		// statement markers and iterate over the values.
		String questionMarks = "(?" +
				StringUtils.repeat(", ?", documentIds.size() - 1) +
				")";
		String query = "SELECT CONCAT(`a`.`value`, \"-\", `b`.`value`)" +
				" FROM " + ElementType.Token + " AS `a`" +
				"     LEFT JOIN " + ElementType.Token + " AS `b`" +
				"         ON `a`.`id` = `b`.`previousTokenId`" +
				" WHERE `a`.`documentId` IN " + questionMarks + ";";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			int counter = 1;
			for (String documentId : documentIds)
			{
				aStatement.setString(counter++, documentId);
			}

			ResultSet result = aStatement.executeQuery();

			while (result.next()) {
				String nextValue = result.getString(1);
				if (nextValue == null) {
					continue;
				}
				biGramList.add(nextValue);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return biGramList;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		List<String> triGramList = new ArrayList<>();
		String query = "SELECT CONCAT(`a`.`value`, \"-\", `b`.`value`, \"-\", `c`.`value`)" +
				" FROM " + ElementType.Token + " AS `a`" +
				"     LEFT JOIN " + ElementType.Token + " AS `b`" +
				"         ON `a`.`id` = `b`.`previousTokenId`" +
				"         LEFT JOIN " + ElementType.Token + " AS `c`" +
				"             ON `b`.`id` = `c`.`previousTokenId`" +
				" WHERE `a`.`documentId` = ?;";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			aStatement.setString(1, documentId);

			ResultSet result = aStatement.executeQuery();

			while (result.next()) {
				String nextValue = result.getString(1);
				if (nextValue == null) {
					continue;
				}
				triGramList.add(nextValue);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return triGramList;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		List<String> triGramList = new ArrayList<>();
		String query = "SELECT CONCAT(`a`.`value`, \"-\", `b`.`value`, \"-\", `c`.`value`)" +
				" FROM " + ElementType.Token + " AS `a`" +
				"     LEFT JOIN " + ElementType.Token + " AS `b`" +
				"         ON `a`.`id` = `b`.`previousTokenId`" +
				"         LEFT JOIN " + ElementType.Token + " AS `c`" +
				"             ON `b`.`id` = `c`.`previousTokenId`;";
		try (Statement aStatement = this.connection.createStatement())
		{
			ResultSet result = aStatement.executeQuery(query);

			while (result.next()) {
				String nextValue = result.getString(1);
				if (nextValue == null) {
					continue;
				}
				triGramList.add(nextValue);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return triGramList;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		List<String> triGramList = new ArrayList<>();

		// Since the java driver does not support preparing statements with
		// arrays of values, we have to generate a fitting amount of prepared
		// statement markers and iterate over the values.
		String questionMarks = "(?" +
				StringUtils.repeat(", ?", documentIds.size() - 1) +
				")";
		String query = "SELECT CONCAT(`a`.`value`, \"-\", `b`.`value`, \"-\", `c`.`value`)" +
				" FROM " + ElementType.Token + " AS `a`" +
				"     LEFT JOIN " + ElementType.Token + " AS `b`" +
				"         ON `a`.`id` = `b`.`previousTokenId`" +
				"         LEFT JOIN " + ElementType.Token + " AS `c`" +
				"             ON `b`.`id` = `c`.`previousTokenId`" +
				" WHERE `a`.`documentId` IN " + questionMarks + ";";
		try (PreparedStatement aStatement =
				     this.connection.prepareStatement(query))
		{
			int counter = 1;
			for (String documentId : documentIds)
			{
				aStatement.setString(counter++, documentId);
			}

			ResultSet result = aStatement.executeQuery();

			while (result.next()) {
				String nextValue = result.getString(1);
				if (nextValue == null) {
					continue;
				}
				triGramList.add(nextValue);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return triGramList;
	}
}
