package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.EdgeDefinition;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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
	public String storeJCasDocument(JCas document)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		BaseDocument docObject = new BaseDocument(documentId);
		docObject.addAttribute("id", documentId);
		docObject.addAttribute("text", document.getDocumentText());
		docObject.addAttribute("language", document.getDocumentLanguage());

		this.graph.vertexCollection(ElementType.Document.toString())
				.insertVertex(docObject);

		return documentId;
	}

	@Override
	public String storeParagraph(
			Paragraph paragraph, String documentId, String previousParagraphId
	)
	{
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
				ElementType.Document + "/" + documentId, paragraphObject.getId()
		);
		this.graph.edgeCollection(Relationship.DocumentHasParagraph.toString())
				.insertEdge(documentHasParagraphEdge);

		// If a previous Paragraph was given, add an Edge from it to the current
		// one.
		if (previousParagraphId != null)
		{
			// Create Edge object from previous Paragraph to current one
			// and insert into graph
			BaseEdgeDocument nextParagraphEdge = new BaseEdgeDocument(
					previousParagraphId,
					paragraphObject.getId()
			);
			this.graph.edgeCollection(Relationship.NextParagraph.toString())
					.insertEdge(nextParagraphEdge);
		}

		return paragraphObject.getId();
	}

	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId,
			String previousSentenceId
	)
	{
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
				ElementType.Document + "/" + documentId, sentenceObject.getId()
		);
		this.graph.edgeCollection(Relationship.DocumentHasSentence.toString())
				.insertEdge(documentHasSentenceEdge);

		// Create Edge object from Sentence to Paragraph and insert
		BaseEdgeDocument sentenceInParagraphEdge = new BaseEdgeDocument(
				sentenceObject.getId(), paragraphId
		);
		this.graph.edgeCollection(Relationship.SentenceInParagraph.toString())
				.insertEdge(sentenceInParagraphEdge);

		// If a previous Sentence was given, add an Edge from it to the current
		// one.
		if (previousSentenceId != null)
		{
			// Create Edge object from previous Sentence to current one
			// and insert into graph
			BaseEdgeDocument nextSentenceEdge = new BaseEdgeDocument(
					previousSentenceId,
					sentenceObject.getId()
					);
			this.graph.edgeCollection(Relationship.NextSentence.toString())
					.insertEdge(nextSentenceEdge);
		}

		return sentenceObject.getId();
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
		BaseEdgeDocument documentHasTokenEdge = new BaseEdgeDocument(
				ElementType.Document + "/" + documentId, tokenObject.getId()
		);
		this.graph.edgeCollection(Relationship.DocumentHasToken.toString())
				.insertEdge(documentHasTokenEdge);

		// Create Edge object from Token to Paragraph and insert
		BaseEdgeDocument tokenInParagraphEdge = new BaseEdgeDocument(
				tokenObject.getId(), paragraphId
		);
		this.graph.edgeCollection(Relationship.TokenInParagraph.toString())
				.insertEdge(tokenInParagraphEdge);

		// Create Edge object from Token to Sentence and insert
		BaseEdgeDocument tokenInSentenceEdge = new BaseEdgeDocument(
				tokenObject.getId(), sentenceId
		);
		this.graph.edgeCollection(Relationship.TokenInSentence.toString())
				.insertEdge(tokenInSentenceEdge);

		// Create edge from Token to Lemma and insert into graph
		String lemmaId = this.getLemmaId(token.getLemma().getValue());
		BaseEdgeDocument tokenHasLemmaEdge = new BaseEdgeDocument(
				tokenObject.getId(), lemmaId
		);
		this.graph.edgeCollection(Relationship.TokenHasLemma.toString())
				.insertEdge(tokenHasLemmaEdge);

		// Check, if an edge from Document to Lemma already exists.
		// This happens, if the Lemma already occured in the current document.
		Map<String, Object> edgeParams = new HashMap<>();
		edgeParams.put(
				"documentId", ElementType.Document + "/" + documentId
		);
		edgeParams.put(
				"lemmaId", lemmaId
		);
		String edgeQuery = "FOR e IN " + Relationship.DocumentHasLemma + " " +
				"FILTER e._from == @documentId && e._to == @lemmaId " +
				"RETURN e";
		ArangoCursor<BaseEdgeDocument> edgeResult = this.db.query(
				edgeQuery, edgeParams, null, BaseEdgeDocument.class
		);
		if (!edgeResult.hasNext())
		{
			// Edge does not yet exist, so create it and insert into graph
			BaseEdgeDocument documentHasLemmaEdge = new BaseEdgeDocument(
					ElementType.Document + "/" + documentId, lemmaId
			);
			this.graph.edgeCollection(Relationship.DocumentHasLemma.toString())
					.insertEdge(documentHasLemmaEdge);
		}

		// Create POS object and insert into collection
		BaseDocument posObject = new BaseDocument();
		posObject.addAttribute("value", token.getPos().getPosValue());
		this.graph.vertexCollection(ElementType.Pos.toString())
				.insertVertex(posObject);
		// Create edge from Token to POS and insert into graph
		BaseEdgeDocument tokenAtPosEdge = new BaseEdgeDocument(
				tokenObject.getId(), posObject.getId()
		);
		this.graph.edgeCollection(Relationship.TokenAtPos.toString())
				.insertEdge(tokenAtPosEdge);

		// If a previous Token was given, add an Edge from it to the current
		// one.
		if (previousTokenId != null)
		{
			// Create Edge object from previous Token to current one
			// and insert into graph
			BaseEdgeDocument nextTokenEdge = new BaseEdgeDocument(
					previousTokenId,
					tokenObject.getId()
			);
			this.graph.edgeCollection(Relationship.NextToken.toString())
					.insertEdge(nextTokenEdge);
		}

		return tokenObject.getId();
	}

	/**
	 * Creates a new Lemma if none with the given value exists.
	 * Otherwise retrieves the existing one.
	 * @param value The Lemma's value.
	 * @return The Lemma's id.
	 */
	protected String getLemmaId(String value) {
		// Check if a Lemma with given value already exists since Lemmata
		// should be reused.
		String lemmaQuery = "FOR l in " + ElementType.Lemma + " FILTER l.value == @lemmaValue RETURN l";
		Map<String, Object> lemmaParams = new HashMap<>();
		lemmaParams.put("lemmaValue", value);
		ArangoCursor<BaseDocument> lemmaResult = this.db.query(
				lemmaQuery, lemmaParams, null, BaseDocument.class
		);
		BaseDocument lemmaObject;
		if (lemmaResult.hasNext())
		{
			// If a Lemma was found, reuse it.
			lemmaObject = lemmaResult.next();
		} else {
			// If not, create a new Lemma object and insert into collection.
			lemmaObject = new BaseDocument();
			lemmaObject.addAttribute("value", value);
			this.graph.vertexCollection(ElementType.Lemma.toString())
					.insertVertex(lemmaObject);
		}

		return lemmaObject.getId();
	}

	@Override
	public void checkIfDocumentExists(String documentId)
			throws DocumentNotFoundException
	{
		if (!this.db.collection(ElementType.Document.toString())
			.documentExists(documentId))
		{
			throw new DocumentNotFoundException();
		}
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		String query = "FOR d IN " + ElementType.Document + " RETURN d";
		ArangoCursor<BaseDocument> result = this.db.query(
				query, null, null, BaseDocument.class
		);

		return StreamSupport
				.stream(result.spliterator(), false)
				.map(BaseDocument::getKey)
				.collect(Collectors.toCollection(ArrayList::new));
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
			throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String query = "WITH " + ElementType.Document + ", " + Relationship.DocumentHasLemma + ", " + ElementType.Lemma + " " +
				"FOR lemma IN OUTBOUND @documentId " + Relationship.DocumentHasLemma + " " +
				"RETURN DISTINCT lemma";
		Map<String, Object> bindParams = new HashMap<>();
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

			if (documentObject == null)
				throw new DocumentNotFoundException();

			DocumentMetaData meta = DocumentMetaData.create(aCAS);
			meta.setDocumentId(documentObject.getAttribute("id")
					.toString());
			aCAS.setDocumentLanguage(documentObject.getAttribute("language")
					.toString());
			aCAS.setDocumentText(documentObject.getAttribute("text")
					.toString());

			// query all Tokens in the Document
			String tokenQuery = "WITH " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + " " +
					"FOR token IN OUTBOUND @documentId " + Relationship.DocumentHasToken + " " +
					"RETURN DISTINCT token";
			Map<String, Object> bindParams = new HashMap<>();
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
				String lemmaQuery = "WITH " + ElementType.Token + ", " + Relationship.TokenHasLemma + ", " + ElementType.Lemma + " " +
						"FOR lemma IN OUTBOUND @tokenId " + Relationship.TokenHasLemma + " " +
						"RETURN DISTINCT lemma";
				Map<String, Object> lemmaParams = new HashMap<>();
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
				String posQuery = "WITH " + ElementType.Token + ", " + Relationship.TokenAtPos + ", " + ElementType.Pos + " " +
						"FOR pos IN OUTBOUND @tokenId " + Relationship.TokenAtPos + " " +
						"RETURN DISTINCT pos";
				Map<String, Object> posParams = new HashMap<>();
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
		String query = "WITH " + ElementType.Lemma + ", " + Relationship.DocumentHasLemma + ", " + ElementType.Document + " " +
				"FOR lemma IN " + ElementType.Lemma + " " +
				"    FILTER lemma.value == @lemmaValue " +
				"    LET docCount = LENGTH( " +
				"        FOR document IN INBOUND lemma " + Relationship.DocumentHasLemma + " " +
				"            RETURN DISTINCT document " +
				"    ) " +
				"RETURN {'count': docCount}";
		Map<String, Object> bindParam = new HashMap<>();

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
		String query = "RETURN {'count': LENGTH(" + type + ")}";
		ArangoCursor<BaseDocument> result = this.db.query(
				query, null, null, BaseDocument.class
		);
		return Integer.parseInt(
				result.next().getAttribute("count").toString()
		);
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws TypeNotCountableException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String query = "WITH " + ElementType.Document + ", " + type + ", " + this.getRelationshipFromDocumentToType(type) + " " +
				"FOR element " +
				"   IN OUTBOUND @documentId " +
				"   " + this.getRelationshipFromDocumentToType(type) + " " +
				"   COLLECT WITH COUNT INTO count " +
				"   RETURN {'count':count}";
		Map<String, Object> bindParams = new HashMap<>();
		bindParams.put(
				"documentId",
				ElementType.Document.toString() + "/" + documentId
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
			throws TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		String query = "FOR element IN " + type +
				"   FILTER element.value == @value " +
				"   COLLECT WITH COUNT INTO count " +
				"RETURN {'count':count}";
		Map<String, Object> bindParam = new HashMap<>();
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
	) throws DocumentNotFoundException, TypeNotCountableException,
			TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		this.checkIfDocumentExists(documentId);
		String query = "WITH " + ElementType.Document + ", " + type + ", " + this.getRelationshipFromDocumentToType(type) + " " +
				"FOR element " +
				"   IN OUTBOUND @documentId " +
				"   " + this.getRelationshipFromDocumentToType(type) + " " +
				"   FILTER element.value == @value " +
				"   COLLECT WITH COUNT INTO count " +
				"   RETURN {'count':count}";
		Map<String, Object> bindParams = new HashMap<>();
		bindParams.put(
				"documentId",
				ElementType.Document.toString() + "/" + documentId
		);
		bindParams.put("value", value);

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
	 * count in how many Tokens a Lemma occurs.
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
		String query = "WITH " + ElementType.Lemma + ", " + Relationship.TokenHasLemma + ", " + ElementType.Token + " " +
				"FOR lemma IN " + ElementType.Lemma + " " +
				"   LET occurenceCount = LENGTH(" +
				"       FOR token IN INBOUND lemma " + Relationship.TokenHasLemma + " " +
				"           RETURN token" +
				"   ) " +
				"   RETURN {'lemma': lemma.value, 'count': occurenceCount}";
		ArangoCursor<BaseDocument> result = this.db.query(
				query, null, null, BaseDocument.class
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

		String query = "WITH " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + ", " + Relationship.DocumentHasLemma + ", " + ElementType.Lemma + " " +
				"FOR document IN " + ElementType.Document + " " +
				"   LET tokenCount = LENGTH(" +
				"       FOR token IN OUTBOUND document " + Relationship.DocumentHasToken + " " +
				"           RETURN DISTINCT token " +
				"   ) " +
				"   LET lemmaCount = LENGTH(" +
				"       FOR lemma IN OUTBOUND document " + Relationship.DocumentHasLemma + " " +
				"           RETURN DISTINCT lemma " +
				"   ) " +
				"   RETURN {'document': document._key, 'ttr': (lemmaCount/tokenCount)}";

		ArangoCursor<BaseDocument> result = this.db.query(
				query, null, null, BaseDocument.class
		);

		while (result.hasNext())
		{
			BaseDocument ttr = result.next();
			if (ttr.getAttribute("ttr") != null)
			{
				documentTTRMap.put(
						ttr.getAttribute("document").toString(),
						Double.parseDouble(ttr.getAttribute("ttr").toString())
				);
			}
		}

		return documentTTRMap;
	}

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
		Map<String, Double> documentTTRMap = new ConcurrentHashMap<>();

		String query = "WITH " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + ", " + Relationship.DocumentHasLemma + ", " + ElementType.Lemma + " " +
				"FOR document IN " + ElementType.Document + " " +
				"   FILTER document._key IN @documentIds " +
				"   LET tokenCount = LENGTH(" +
				"       FOR token IN OUTBOUND document " + Relationship.DocumentHasToken + " " +
				"           RETURN DISTINCT token " +
				"   ) " +
				"   LET lemmaCount = LENGTH(" +
				"       FOR lemma IN OUTBOUND document " + Relationship.DocumentHasLemma + " " +
				"           RETURN DISTINCT lemma " +
				"   ) " +
				"   RETURN {'document': document._key, 'ttr': (lemmaCount/tokenCount)}";
		Map<String, Object> bindParams = new HashMap<>();
		bindParams.put("documentIds", documentIds);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, bindParams, null, BaseDocument.class
		);

		while (result.hasNext())
		{
			BaseDocument ttr = result.next();
			if (ttr.getAttribute("ttr") == null) {
				documentTTRMap.put(
						ttr.getAttribute("document").toString(),
						0.0d
				);
			} else {
				documentTTRMap.put(
						ttr.getAttribute("document").toString(),
						Double.parseDouble(ttr.getAttribute("ttr").toString())
				);
			}
		}

		return documentTTRMap;
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		Map<String, Integer> termFrequencyMap = new ConcurrentHashMap<>();

		String query = "WITH " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + ", " + Relationship.TokenHasLemma + ", " + ElementType.Lemma + " " +
				"FOR lemma IN 2 OUTBOUND @documentId " + Relationship.DocumentHasToken + ", " + Relationship.TokenHasLemma + " " +
				"    COLLECT value = lemma.value WITH COUNT INTO count " +
				"RETURN {'lemma': value, 'count': count}";
		Map<String, Object> queryParams = new HashMap<>();
		queryParams.put(
				"documentId",
				ElementType.Document + "/" + documentId
		);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, queryParams, null, BaseDocument.class
		);

		while (result.hasNext())
		{
			BaseDocument document = result.next();
			termFrequencyMap.put(
					document.getAttribute("lemma").toString(),
					Integer.parseInt(document.getAttribute("count").toString())
			);
		}

		return termFrequencyMap;
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String query = "WITH " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + ", " + Relationship.TokenHasLemma + ", " + ElementType.Lemma + " " +
				"FOR lemma IN 2 OUTBOUND @documentId " + Relationship.DocumentHasToken + ", " + Relationship.TokenHasLemma + " " +
				"    FILTER lemma.value == @lemmaValue " +
				"    COLLECT value = lemma.value WITH COUNT INTO count " +
				"RETURN {'lemma': value, 'count': count}";
		Map<String, Object> queryParams = new HashMap<>();
		queryParams.put(
				"documentId",
				ElementType.Document + "/" + documentId
		);
		queryParams.put(
				"lemmaValue", lemma
		);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, queryParams, null, BaseDocument.class
		);

		if (!result.hasNext())
		{
			return 0;
		}
		return Integer.parseInt(
				result.next().getAttribute("count").toString()
		);
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String query = "With " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + " " +
				"FOR t1 IN OUTBOUND @documentId " + Relationship.DocumentHasToken + " " +
				"    FOR t2 IN OUTBOUND t1 " + Relationship.NextToken + " " +
				"        return DISTINCT {'bigram': CONCAT(t1.value, '-', t2.value)}";
		Map<String, Object> params = new HashMap<>();
		params.put("documentId", ElementType.Document + "/" + documentId);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, params, null, BaseDocument.class
		);
		ArrayList<String> biGrams = new ArrayList<>();

		while (result.hasNext())
		{
			BaseDocument element = result.next();
			biGrams.add(element.getAttribute("bigram").toString());
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		String query = "With " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + " " +
				"FOR d IN " + ElementType.Document + " " +
				"    FOR t1 IN OUTBOUND d " + Relationship.DocumentHasToken + " " +
				"        FOR t2 IN OUTBOUND t1 " + Relationship.NextToken + " " +
				"            return DISTINCT {'bigram': CONCAT(t1.value, '-', t2.value)}";

		ArangoCursor<BaseDocument> result = this.db.query(
				query, null, null, BaseDocument.class
		);
		ArrayList<String> biGrams = new ArrayList<>();

		while (result.hasNext())
		{
			BaseDocument element = result.next();
			biGrams.add(element.getAttribute("bigram").toString());
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		String query = "With " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + " " +
				"FOR d IN " + ElementType.Document + " " +
				"    FILTER d._key in @documentKeys " +
				"    FOR t1 IN OUTBOUND d " + Relationship.DocumentHasToken + " " +
				"        FOR t2 IN OUTBOUND t1 " + Relationship.NextToken + " " +
				"            return DISTINCT {'bigram': CONCAT(t1.value, '-', t2.value)}";
		Map<String, Object> params = new HashMap<>();
		params.put("documentKeys", documentIds);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, params, null, BaseDocument.class
		);
		ArrayList<String> biGrams = new ArrayList<>();

		while (result.hasNext())
		{
			BaseDocument element = result.next();
			biGrams.add(element.getAttribute("bigram").toString());
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String query = "With " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + " " +
				"FOR t1 IN OUTBOUND @documentId " + Relationship.DocumentHasToken + " " +
				"    FOR t2 IN OUTBOUND t1 " + Relationship.NextToken + " " +
				"        FOR t3 IN OUTBOUND t2 " + Relationship.NextToken + " " +
				"            return DISTINCT {'trigram': CONCAT(t1.value, '-', t2.value, '-', t3.value)}";
		Map<String, Object> params = new HashMap<>();
		params.put("documentId", ElementType.Document + "/" + documentId);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, params, null, BaseDocument.class
		);
		ArrayList<String> triGrams = new ArrayList<>();

		while (result.hasNext())
		{
			BaseDocument element = result.next();
			triGrams.add(element.getAttribute("trigram").toString());
		}
		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		String query = "With " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + " " +
				"FOR d IN " + ElementType.Document + " " +
				"    FOR t1 IN OUTBOUND d " + Relationship.DocumentHasToken + " " +
				"        FOR t2 IN OUTBOUND t1 " + Relationship.NextToken + " " +
				"            FOR t3 IN OUTBOUND t2 " + Relationship.NextToken + " " +
				"                return DISTINCT {'trigram': CONCAT(t1.value, '-', t2.value, '-', t3.value)}";

		ArangoCursor<BaseDocument> result = this.db.query(
				query, null, null, BaseDocument.class
		);
		ArrayList<String> triGrams = new ArrayList<>();

		while (result.hasNext())
		{
			BaseDocument element = result.next();
			triGrams.add(element.getAttribute("trigram").toString());
		}
		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		String query = "With " + ElementType.Document + ", " + Relationship.DocumentHasToken + ", " + ElementType.Token + " " +
				"FOR d IN " + ElementType.Document + " " +
				"    FILTER d._key IN @documentKeys " +
				"    FOR t1 IN OUTBOUND d " + Relationship.DocumentHasToken + " " +
				"        FOR t2 IN OUTBOUND t1 " + Relationship.NextToken + " " +
				"            FOR t3 IN OUTBOUND t2 " + Relationship.NextToken + " " +
				"                return DISTINCT {'trigram': CONCAT(t1.value, '-', t2.value, '-', t3.value)}";
		Map<String, Object> params = new HashMap<>();
		params.put("documentKeys", documentIds);

		ArangoCursor<BaseDocument> result = this.db.query(
				query, params, null, BaseDocument.class
		);
		ArrayList<String> triGrams = new ArrayList<>();

		while (result.hasNext())
		{
			BaseDocument element = result.next();
			triGrams.add(element.getAttribute("trigram").toString());
		}
		return triGrams;
	}
}
