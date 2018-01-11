package dbtest.queryHandler.implementation;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.EdgeDefinition;
import dbtest.queryHandler.AbstractQueryHandler;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.QHException;
import dbtest.queryHandler.exceptions.TypeNotCountableException;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ArangoDBQueryHandler extends AbstractQueryHandler
{
	public enum Relationship
	{
		DocumentHasParagraph, DocumentHasSentence, DocumentHasToken,
		DocumentHasLemma,
		SentenceInParagraph, TokenInParagraph,
		TokenInSentence,
		TokenHasLemma, TokenAtPos,
		NextParagraph, NextSentence, NextToken
	}

	protected final static String dbName = System.getenv("ARANGODB_DB");
	protected final static String graphName = "uimadatabase";

	protected ArangoDB arangodb;
	protected ArangoDatabase db;
	protected ArangoGraph graph;

	public ArangoDBQueryHandler(ArangoDB arangodb)
	{
		this.arangodb = arangodb;
	}

	/**
	 * Creates a collection for each kind of vertex and for each kind of rela-
	 * tionship (edge).
	 * This makes queries somewhat verbose, but makes traversal efficient.
	 * <p>
	 * The less efficient alternative would be to use heterogeneous collections
	 * for edges and maybe even for vertices. This would make some queries sim-
	 * pler, but more cpu-intensive to execute.
	 */
	@Override
	public void setUpDatabase()
	{
		if (this.arangodb.getDatabases().contains(dbName))
		{
			this.arangodb.db(dbName).drop();
		}

		this.arangodb.createDatabase(dbName);
		this.db = this.arangodb.db(dbName);

		Arrays.stream(ElementType.values()).parallel()
				.map(Enum::toString)
				.forEach(this.db::createCollection);

		Collection<EdgeDefinition> edgeDefinitions = new ArrayList<>();

		// Since each relationship has different from/to definitions, we unfor-
		// tunately can't create them automatically. Except maybe by changing
		// the enum or adding a map for the from/to definitions, but that
		// probably wouldn't make the code better/more readable/anything.
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.DocumentHasParagraph.toString())
						.from(ElementType.Document.toString())
						.to(ElementType.Paragraph.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.DocumentHasSentence.toString())
						.from(ElementType.Document.toString())
						.to(ElementType.Sentence.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.DocumentHasToken.toString())
						.from(ElementType.Document.toString())
						.to(ElementType.Token.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.DocumentHasLemma.toString())
						.from(ElementType.Document.toString())
						.to(ElementType.Lemma.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.SentenceInParagraph.toString())
						.from(ElementType.Sentence.toString())
						.to(ElementType.Paragraph.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.TokenInParagraph.toString())
						.from(ElementType.Token.toString())
						.to(ElementType.Paragraph.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.TokenInSentence.toString())
						.from(ElementType.Token.toString())
						.to(ElementType.Sentence.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.TokenHasLemma.toString())
						.from(ElementType.Token.toString())
						.to(ElementType.Lemma.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.TokenAtPos.toString())
						.from(ElementType.Token.toString())
						.to(ElementType.Pos.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.NextParagraph.toString())
						.from(ElementType.Paragraph.toString())
						.to(ElementType.Paragraph.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.NextSentence.toString())
						.from(ElementType.Sentence.toString())
						.to(ElementType.Sentence.toString())
		);
		edgeDefinitions.add(
				new EdgeDefinition()
						.collection(Relationship.NextToken.toString())
						.from(ElementType.Token.toString())
						.to(ElementType.Token.toString())
		);

		this.db.createGraph(
				graphName, edgeDefinitions
		);
		this.graph = this.db.graph(graphName);
	}

	/**
	 * Since setUpDatabase drops an existing database before creating a new one,
	 * this can be an alias for setUpDatabase.
	 * <p>
	 * Dropping is also easier than deleting everything by query.
	 */
	@Override
	public void clearDatabase()
	{
		this.setUpDatabase();
	}

	@Override
	public void storeJCasDocument(JCas document)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		BaseDocument docObject = new BaseDocument(documentId);
		docObject.addAttribute("id", documentId);
		docObject.addAttribute("text", document.getDocumentText());
		docObject.addAttribute("language", document.getDocumentLanguage());

		this.graph.vertexCollection(ElementType.Document.toString())
				.insertVertex(docObject);

		/*
		 * Store each element of the jCas that was annotated as a Para-
		 * graph.
		 */
		Paragraph previousParagraph = null;
		for (Paragraph paragraph
				: JCasUtil.select(document, Paragraph.class))
		{
			this.storeParagraph(paragraph, document, previousParagraph);
			previousParagraph = paragraph;
		}
	}

	@Override
	public void storeJCasDocuments(Iterable<JCas> documents)
	{
		for (JCas document : documents)
		{
			this.storeJCasDocument(document);
		}
	}

	@Override
	public void storeParagraph(
			Paragraph paragraph, JCas document, Paragraph previousParagraph
	)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		// Create Paragraph object for insertion
		BaseDocument paragraphObject = new BaseDocument();
		paragraphObject.addAttribute("documentId", documentId);
		paragraphObject.addAttribute("begin", paragraph.getBegin());
		paragraphObject.addAttribute("end", paragraph.getEnd());

		// Insert Paragraph object
		this.graph.vertexCollection(ElementType.Paragraph.toString())
				.insertVertex(paragraphObject);

		// Create Edge object from Document to Paragraph and insert
		BaseEdgeDocument documentHasParagraphEdge = new BaseEdgeDocument(
				documentId, paragraphObject.getKey()
		);
		this.graph.edgeCollection(Relationship.DocumentHasParagraph.toString())
				.insertEdge(documentHasParagraphEdge);

		if (previousParagraph != null)
		{
			// Query Paragraph object for previous Paragraph
			String query = "FOR p IN " + ElementType.Paragraph + " FILTER p.documentId == @documentId FILTER p.begin == @begin FILTER p.end == @end RETURN p";

			Map<String, Object> bindParams = new HashMap<>();
			bindParams.put("documentId", documentId);
			bindParams.put("begin", previousParagraph.getBegin());
			bindParams.put("end", previousParagraph.getEnd());

			ArangoCursor<BaseDocument> result = this.db.query(
					query, bindParams, null, BaseDocument.class
			);
			if (result.hasNext())
			{
				BaseDocument previousParagraphObject = result.next();

				// Create Edge object from previous Paragraph to current one
				// and insert into graph
				BaseEdgeDocument nextParagraphEdge = new BaseEdgeDocument(
						paragraphObject.getKey(),
						previousParagraphObject.getKey()
				);
				this.graph.edgeCollection(Relationship.NextParagraph.toString())
						.insertEdge(nextParagraphEdge);
			}
		}

		/*
		 * Store each element of the jCas that was annotated as a Sen-
		 * tence.
		 */
		Sentence previousSentence = null;
		for (Sentence sentence : JCasUtil.selectCovered(
				document,
				Sentence.class, paragraph
		))
		{
			this.storeSentence(sentence, document, paragraph, previousSentence);
			previousSentence = sentence;
		}
	}

	@Override
	public void storeParagraph(Paragraph paragraph, JCas document)
	{
		this.storeParagraph(paragraph, document, null);
	}

	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph,
			Sentence previousSentence
	)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		// Create Sentence object for insertion
		BaseDocument sentenceObject = new BaseDocument();
		sentenceObject.addAttribute("documentId", documentId);
		sentenceObject.addAttribute("begin", sentence.getBegin());
		sentenceObject.addAttribute("end", sentence.getEnd());

		// Insert Sentence object
		this.graph.vertexCollection(ElementType.Sentence.toString())
				.insertVertex(sentenceObject);

		// Create Edge object from Document to Sentence and insert
		BaseEdgeDocument documentHasSentenceEdge = new BaseEdgeDocument(
				documentId, sentenceObject.getKey()
		);
		this.graph.edgeCollection(Relationship.DocumentHasSentence.toString())
				.insertEdge(documentHasSentenceEdge);

		// Query Paragraph Object to add an edge from the Sentence
		String paragraphQuery = "FOR p IN " + ElementType.Paragraph + " FILTER p.documentId == @documentId FILTER p.begin == @begin FILTER p.end == @end RETURN p";
		Map<String, Object> paragraphParams = new HashMap<>();
		paragraphParams.put("documentId", documentId);
		paragraphParams.put("begin", paragraph.getBegin());
		paragraphParams.put("end", paragraph.getEnd());
		ArangoCursor<BaseDocument> paragraphResult = this.db.query(
				paragraphQuery, paragraphParams, null, BaseDocument.class
		);
		BaseDocument paragraphObject = paragraphResult.next();

		// Create Edge object from Sentence to Paragraph and insert
		BaseEdgeDocument sentenceInParagraphEdge = new BaseEdgeDocument(
				sentenceObject.getKey(), paragraphObject.getKey()
		);
		this.graph.edgeCollection(Relationship.SentenceInParagraph.toString())
				.insertEdge(sentenceInParagraphEdge);

		if (previousSentence != null)
		{
			// Query Sentence object for previous Sentence
			String sentenceQuery = "FOR p IN " + ElementType.Sentence + " FILTER p.documentId == @documentId FILTER p.begin == @begin FILTER p.end == @end RETURN p";
			Map<String, Object> bindParams = new HashMap<>();
			bindParams.put("documentId", documentId);
			bindParams.put("begin", previousSentence.getBegin());
			bindParams.put("end", previousSentence.getEnd());
			ArangoCursor<BaseDocument> sentenceResult = this.db.query(
					sentenceQuery, bindParams, null, BaseDocument.class
			);
			BaseDocument previousSentenceObject = sentenceResult.next();

			// Create Edge object from previous Sentence to current one
			// and insert into graph
			BaseEdgeDocument nextSentenceEdge = new BaseEdgeDocument(
					sentenceObject.getKey(),
					previousSentenceObject.getKey()
			);
			this.graph.edgeCollection(Relationship.NextSentence.toString())
					.insertEdge(nextSentenceEdge);
		}

		/*
		 * Store each element of the jCas that was annotated as a Token.
		 */
		Token previousToken = null;
		for (Token token
				: JCasUtil.selectCovered(document, Token.class, sentence))
		{
			this.storeToken(
					token,
					document,
					paragraph,
					sentence,
					previousToken
			);
			previousToken = token;
		}
	}

	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph
	)
	{
		this.storeSentence(sentence, document, paragraph, null);
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
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		// Create Token object for insertion
		BaseDocument tokenObject = new BaseDocument();
		tokenObject.addAttribute("documentId", documentId);
		tokenObject.addAttribute("begin", token.getBegin());
		tokenObject.addAttribute("end", token.getEnd());
		tokenObject.addAttribute("value", token.getCoveredText());

		// Insert Token object
		this.graph.vertexCollection(ElementType.Token.toString())
				.insertVertex(tokenObject);

		// Create Edge object from Document to Token and insert
		BaseEdgeDocument documentHasSentenceEdge = new BaseEdgeDocument(
				documentId, tokenObject.getKey()
		);
		this.graph.edgeCollection(Relationship.DocumentHasSentence.toString())
				.insertEdge(documentHasSentenceEdge);

		// Query Paragraph Object to add an edge from the Token
		String paragraphQuery = "FOR p IN " + ElementType.Paragraph + " FILTER p.documentId == @documentId FILTER p.begin == @begin FILTER p.end == @end RETURN p";
		Map<String, Object> paragraphParams = new HashMap<>();
		paragraphParams.put("documentId", documentId);
		paragraphParams.put("begin", paragraph.getBegin());
		paragraphParams.put("end", paragraph.getEnd());
		ArangoCursor<BaseDocument> paragraphResult = this.db.query(
				paragraphQuery, paragraphParams, null, BaseDocument.class
		);
		BaseDocument paragraphObject = paragraphResult.next();

		// Create Edge object from Token to Paragraph and insert
		BaseEdgeDocument tokenInParagraphEdge = new BaseEdgeDocument(
				tokenObject.getKey(), paragraphObject.getKey()
		);
		this.graph.edgeCollection(Relationship.TokenInParagraph.toString())
				.insertEdge(tokenInParagraphEdge);

		// Query Sentence Object to add an edge from the Token
		String sentenceQuery = "FOR p IN " + ElementType.Sentence + " FILTER p.documentId == @documentId FILTER p.begin == @begin FILTER p.end == @end RETURN p";
		Map<String, Object> sentenceParams = new HashMap<>();
		sentenceParams.put("documentId", documentId);
		sentenceParams.put("begin", sentence.getBegin());
		sentenceParams.put("end", sentence.getEnd());
		ArangoCursor<BaseDocument> sentenceResult = this.db.query(
				sentenceQuery, sentenceParams, null, BaseDocument.class
		);
		BaseDocument sentenceObject = sentenceResult.next();

		// Create Edge object from Token to Sentence and insert
		BaseEdgeDocument tokenInSentenceEdge = new BaseEdgeDocument(
				tokenObject.getKey(), sentenceObject.getKey()
		);
		this.graph.edgeCollection(Relationship.TokenInSentence.toString())
				.insertEdge(tokenInSentenceEdge);

		// Create Lemma object and insert into collection
		BaseDocument lemmaObject = new BaseDocument();
		lemmaObject.addAttribute("value", token.getLemma().getValue());
		this.graph.vertexCollection(ElementType.Lemma.toString())
				.insertVertex(lemmaObject);
		// Create edge from Token to Lemma and insert into graph
		BaseEdgeDocument tokenHasLemmaEdge = new BaseEdgeDocument(
				tokenObject.getKey(), lemmaObject.getKey()
		);
		this.graph.edgeCollection(Relationship.TokenHasLemma.toString())
				.insertEdge(tokenHasLemmaEdge);
		// Create edge from Document to Lemma and insert into graph
		BaseEdgeDocument documentHasLemmaEdge = new BaseEdgeDocument(
				documentId, lemmaObject.getKey()
		);
		this.graph.edgeCollection(Relationship.DocumentHasLemma.toString())
				.insertEdge(documentHasLemmaEdge);

		// Create POS object and insert into collection
		BaseDocument posObject = new BaseDocument();
		posObject.addAttribute("value", token.getPos().getPosValue());
		this.graph.vertexCollection(ElementType.Pos.toString())
				.insertVertex(posObject);
		// Create edge from Token to POS and insert into graph
		BaseEdgeDocument tokenAtPosEdge = new BaseEdgeDocument(
				tokenObject.getKey(), posObject.getKey()
		);
		this.graph.edgeCollection(Relationship.TokenAtPos.toString())
				.insertEdge(tokenAtPosEdge);

		if (previousToken != null)
		{
			// Query Token object for previous Token
			String tokenQuery = "FOR p IN " + ElementType.Token + " FILTER p.documentId == @documentId FILTER p.begin == @begin FILTER p.end == @end FILTER p.value == @value RETURN p";
			Map<String, Object> bindParams = new HashMap<>();
			bindParams.put("documentId", documentId);
			bindParams.put("begin", previousToken.getBegin());
			bindParams.put("end", previousToken.getEnd());
			bindParams.put("value", previousToken.getCoveredText());
			ArangoCursor<BaseDocument> tokenResult = this.db.query(
					tokenQuery, bindParams, null, BaseDocument.class
			);
			BaseDocument previouseTokenObject = tokenResult.next();

			// Create Edge object from previous Token to current one
			// and insert into graph
			BaseEdgeDocument nextTokenEdge = new BaseEdgeDocument(
					tokenObject.getKey(),
					previouseTokenObject.getKey()
			);
			this.graph.edgeCollection(Relationship.NextToken.toString())
					.insertEdge(nextTokenEdge);
		}
	}

	@Override
	public void storeToken(
			Token token, JCas document, Paragraph paragraph, Sentence sentence
	)
	{
		this.storeToken(token, document, paragraph, sentence, null);
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		String query = "FOR d IN " + ElementType.Document + " RETURN d";
		ArangoCursor<BaseDocument> result = this.db.query(
				query, null, null, BaseDocument.class
		);

		Stream<String> keyStream = StreamSupport
				.stream(result.spliterator(), false)
				.map(BaseDocument::getKey);

		return keyStream::iterator;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
	{
		String query = "WITH @documentCollection, @documentHasLemma, @lemmaCollection " +
				"FOR lemma IN OUTBOUND @documentId @documentHasLemma " +
				"RETURN DISTINCT lemma";
		Map<String, Object> bindParams = new HashMap<>();
		bindParams.put(
				"documentCollection",
				ElementType.Document.toString()
		);
		bindParams.put(
				"documentHasLemma", Relationship.DocumentHasLemma.toString()
		);
		bindParams.put(
				"lemmaCollection",
				ElementType.Lemma.toString()
		);
		bindParams.put(
				"documentId", ElementType.Document + "/" + documentId
		);
		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParams, null, BaseDocument.class
		);
		return StreamSupport.stream(result.spliterator(), false)
				.map(lemmaObject ->
						lemmaObject.getAttribute("value").toString())
				.collect(Collectors.toCollection(HashSet::new));
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{
		try
		{
			BaseDocument documentObject = this.db
					.collection(ElementType.Document.toString())
					.getDocument(documentId, BaseDocument.class);

			DocumentMetaData meta = DocumentMetaData.create(aCAS);
			meta.setDocumentId(documentObject.getAttribute("id")
					.toString());
			aCAS.setDocumentLanguage(documentObject.getAttribute("language")
					.toString());
			aCAS.setDocumentText(documentObject.getAttribute("text")
					.toString());

			// query all Tokens in the Document
			String tokenQuery = "WITH @documentCollection, @documentHasToken, @tokenCollection " +
					"FOR token IN OUTBOUND @documentId @documentHasToken " +
					"RETURN DISTINCT token";
			Map<String, Object> bindParams = new HashMap<>();
			bindParams.put(
					"documentCollection", ElementType.Document.toString()
			);
			bindParams.put(
					"documentHasToken",
					Relationship.DocumentHasToken.toString()
			);
			bindParams.put(
					"tokenCollection", ElementType.Token.toString()
			);
			bindParams.put(
					"documentId",
					ElementType.Document.toString() + "/" + documentId
			);
			ArangoCursor<BaseDocument> result = this.db.query(
					tokenQuery, bindParams, null, BaseDocument.class
			);

			// iterate over Tokens
			while (result.hasNext())
			{
				BaseDocument tokenObject = result.next();

				Token xmiToken = new Token(
						aCAS.getJCas(),
						Integer.parseInt(tokenObject.getAttribute("begin").toString()),
						Integer.parseInt(tokenObject.getAttribute("end").toString())
				);

				// query Lemmata connected to Token (probably only one)
				String lemmaQuery = "WITH @tokenCollection, @tokenHasLemma, @lemmaCollection " +
						"FOR lemma IN OUTBOUND @tokenId @tokenHasLemma " +
						"RETURN DISTINCT lemma";
				Map<String, Object> lemmaParams = new HashMap<>();
				lemmaParams.put(
						"tokenCollection", ElementType.Token.toString()
				);
				lemmaParams.put(
						"tokenHasLemma",
						Relationship.TokenHasLemma.toString()
				);
				lemmaParams.put(
						"lemmaCollection", ElementType.Lemma.toString()
				);
				lemmaParams.put("tokenId", tokenObject.getId());
				ArangoCursor<BaseDocument> lemmaResult = this.db.query(
						lemmaQuery, lemmaParams, null, BaseDocument.class
				);
				while (lemmaResult.hasNext())
				{
					BaseDocument lemmaObject = lemmaResult.next();
					Lemma lemma = new Lemma(
							aCAS.getJCas(),
							xmiToken.getBegin(),
							xmiToken.getEnd()
					);
					lemma.setValue(lemmaObject.getAttribute("value").toString());
					lemma.addToIndexes();
					xmiToken.setLemma(lemma);
				}

				// query POS connected to Token (probably only one)
				String posQuery = "WITH @tokenCollection, @tokenAtPos, @posCollection " +
						"FOR pos IN OUTBOUND @tokenId @tokenAtPos " +
						"RETURN DISTINCT pos";
				Map<String, Object> posParams = new HashMap<>();
				posParams.put(
						"tokenCollection", ElementType.Token.toString()
				);
				posParams.put(
						"tokenAtPos",
						Relationship.TokenAtPos.toString()
				);
				posParams.put("posCollection", ElementType.Pos.toString());
				posParams.put("tokenId", tokenObject.getId());
				ArangoCursor<BaseDocument> posResult = this.db.query(
						posQuery, posParams, null, BaseDocument.class
				);
				while (posResult.hasNext())
				{
					BaseDocument posObject = posResult.next();
					POS pos = new POS(
							aCAS.getJCas(),
							xmiToken.getBegin(),
							xmiToken.getEnd()
					);
					pos.setPosValue(posObject.getAttribute("value").toString());
					pos.addToIndexes();
					xmiToken.setPos(pos);
				}

				xmiToken.addToIndexes();
			}
		} catch (CASException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		String query = "WITH @lemmaCollection, @documentHasLemma, @documentCollection " +
				"FOR lemma IN @lemmaCollection " +
				"FILTER lemma.value == @lemmaValue " +
				"   FOR document IN INBOUND lemma @documentHasLemma " +
				"RETURN {'count': LENGTH(lemma)}";
		Map<String, Object> bindParam = new HashMap<>();
		bindParam.put("lemmaCollection", ElementType.Lemma.toString());
		bindParam.put(
				"documentHasLemma",
				Relationship.DocumentHasLemma.toString()
		);
		bindParam.put("documentCollection", ElementType.Document.toString());
		bindParam.put("lemmaValue", lemma);
		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParam, null, BaseDocument.class
		);

		return Integer.parseInt(
				result.next().getAttribute("count").toString()
		);
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		String query = "RETURN {'count': LENGTH(@typeCollection)}";
		Map<String, Object> bindParam = new HashMap<>();
		bindParam.put("typeCollection", type.toString());
		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParam, null, BaseDocument.class
		);
		return Integer.parseInt(
				result.next().getAttribute("count").toString()
		);
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws DocumentNotFoundException, TypeNotCountableException
	{
		String query = "WITH @documentCollection, @typeCollection, @relationship " +
				"FOR element " +
				"   IN OUTBOUND @documentId " +
				"   @relationship " +
				"   COLLECT WITH COUNT INTO count" +
				"   RETURN count";
		Map<String, Object> bindParams = new HashMap<>();
		bindParams.put(
				"documentCollection", ElementType.Document.toString()
		);
		bindParams.put("typeCollection", type.toString());
		bindParams.put(
				"documentId",
				ElementType.Document.toString() + "/" + documentId
		);
		bindParams.put(
				"relationship",
				this.getRelationshipFromDocumentToType(type)
		);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParams, null, BaseDocument.class
		);
		return Integer.parseInt(
				result.next().getAttribute("count").toString()
		);
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException
	{
		String query = "FOR element IN @typeCollection" +
				"   FILTER element.value == @value" +
				"   COLLECT WITH COUNT INTO count" +
				"RETURN count";
		Map<String, Object> bindParam = new HashMap<>();
		bindParam.put("typeCollection", type.toString());
		bindParam.put("value", value);
		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParam, null, BaseDocument.class
		);
		return Integer.parseInt(
				result.next().getAttribute("count").toString()
		);
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException, TypeNotCountableException
	{
		String query = "WITH @documentCollection, @typeCollection, @relationship " +
				"FOR element " +
				"   IN OUTBOUND @documentId " +
				"   @relationship " +
				"   FILTER element.value == @value" +
				"   COLLECT WITH COUNT INTO count" +
				"   RETURN count";
		Map<String, Object> bindParams = new HashMap<>();
		bindParams.put(
				"documentCollection", ElementType.Document.toString()
		);
		bindParams.put("typeCollection", type.toString());
		bindParams.put(
				"documentId",
				ElementType.Document.toString() + "/" + documentId
		);
		bindParams.put("value", value);
		bindParams.put(
				"relationship",
				this.getRelationshipFromDocumentToType(type)
		);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParams, null, BaseDocument.class
		);
		return Integer.parseInt(
				result.next().getAttribute("count").toString()
		);
	}

	/**
	 * @param type The connected Type.
	 * @return The relationship connecting Document to the given Type.
	 * @throws TypeNotCountableException If the given Type is not countable.
	 */
	private Relationship getRelationshipFromDocumentToType(ElementType type)
			throws TypeNotCountableException
	{
		switch (type)
		{
			case Paragraph:
				return Relationship.DocumentHasParagraph;
			case Sentence:
				return Relationship.DocumentHasSentence;
			case Token:
				return Relationship.DocumentHasToken;
			case Lemma:
				return Relationship.DocumentHasLemma;
			default:
				throw new TypeNotCountableException();
		}
	}

	/**
	 * Since every Token is connected to exactly one Document (the one it is
	 * contained in), we do not need to query the Documents and can instead
	 * count, in how many Tokens a Lemma occurs.
	 * <p>
	 * This can only be a problem, if there are dangling Tokens (which do not
	 * have a connection to a Document). However, this should never happen.
	 *
	 * @return The amount of occurences of each Lemma an all Documents.
	 */
	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		Map<String, Integer> lemmaOccurenceCount = new HashMap<>();
		String query = "WITH @lemmaCollection, @tokenHasLemma, @tokenCollection" +
				"FOR lemma IN @lemmaCollection" +
				"   LET occurenceCount = (" +
				"       FOR token IN INBOUND lemma @tokenHasLemma" +
				"           COLLECT WITH COUNT INTO count" +
				"           RETURN count" +
				"   ) " +
				"   RETURN {'lemma': lemma.value, 'count': occurenceCount}";
		Map<String, Object> bindParams = new HashMap<>();
		bindParams.put(
				"lemmaCollection",
				ElementType.Lemma.toString()
		);
		bindParams.put(
				"tokenHasLemma",
				Relationship.TokenHasLemma.toString()
		);
		bindParams.put(
				"tokenCollection",
				ElementType.Token.toString()
		);
		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParams, null, BaseDocument.class
		);

		while (result.hasNext())
		{
			BaseDocument obj = result.next();
			lemmaOccurenceCount.put(
					obj.getAttribute("lemma").toString(),
					Integer.parseInt(obj.getAttribute("count").toString())
			);
		}

		return lemmaOccurenceCount;
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		Map<String, Double> documentTTRMap = new ConcurrentHashMap<>();

		String query = "WITH @documentCollection, @documentHasToken, @tokenCollection, @documentHasLemma, @lemmaCollection " +
				"FOR document IN @documentCollection " +
				"   LET tokenCount = (" +
				"       FOR token IN OUTBOUND document @documentHasToken " +
				"           COLLECT WITH COUNT INTO count " +
				"           RETURN count" +
				"   ) " +
				"   LET lemmaCount = (" +
				"       FOR lemma IN OUTBOUND document @documentHasLemma " +
				"           COLLECT lemma.value WITH COUNT INTO count " +
				"           RETURN count " +
				"   ) " +
				"   RETURN {'document': document.key, 'ttr': (lemmaCount/tokenCount)}";
		Map<String, Object> bindParams = new HashMap<>();
		bindParams.put(
				"documentCollection", ElementType.Document.toString()
		);
		bindParams.put(
				"documentHasToken", Relationship.DocumentHasToken.toString()
		);
		bindParams.put(
				"tokenCollection", ElementType.Token.toString()
		);
		bindParams.put(
				"documentHasLemma", Relationship.DocumentHasLemma.toString()
		);
		bindParams.put(
				"lemmaCollection", ElementType.Lemma.toString()
		);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParams, null, BaseDocument.class
		);

		while (result.hasNext())
		{
			BaseDocument ttr = result.next();
			documentTTRMap.put(
					ttr.getAttribute("document").toString(),
					Double.parseDouble(ttr.getAttribute("ttr").toString())
			);
		}

		return documentTTRMap;
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
	public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException
	{
		return 0;
	}

	@Override
	public double calculateTermFrequencyWithLogNormForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException
	{
		return 0;
	}

	@Override
	public Map<String, Double> calculateTermFrequenciesForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException
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
