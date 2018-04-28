package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.google.common.collect.Maps;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.logging.Level;

public class BlazegraphQueryHandler extends AbstractQueryHandler
{
	protected String rootEndpoint;

	protected enum Model
	{
		Document(
				"document", "http://hucompute.org/TextImager/Model/Document#"
		),
		Paragraph(
				"paragraph", "http://hucompute.org/TextImager/Model/Paragraph#"
		),
		Sentence(
				"sentence", "http://hucompute.org/TextImager/Model/Sentence#"
		),
		Token(
				"token", "http://hucompute.org/TextImager/Model/Token#"
		),
		Lemma(
				"lemma", "http://hucompute.org/TextImager/Model/Lemma#"
		),
		Pos(
				"pos", "http://hucompute.org/TextImager/Model/Pos#"
		);

		protected String name;
		protected String url;

		Model(String name, String url)
		{
			this.name = name;
			this.url = url;
		}

		public String prefix()
		{
			return "PREFIX " + this.name + ": <" + this.url + ">";
		}

		@Override
		public String toString()
		{
			return this.name;
		}
	}

	public enum Relationship
	{
		DocumentHasParagraph(
				"documentHasParagraph", "http://hucompute.org/TextImager/Relationship/DocumentHasParagraph"
		),
		DocumentHasSentence(
				"documentHasSentence", "http://hucompute.org/TextImager/Relationship/DocumentHasSentence"
		),
		DocumentHasToken(
				"documentHasToken", "http://hucompute.org/TextImager/Relationship/DocumentHasToken"
		),
		DocumentHasLemma(
				"documentHasLemma", "http://hucompute.org/TextImager/Relationship/DocumentHasLemma"
		),
		SentenceInParagraph(
				"sentenceInParagraph", "http://hucompute.org/TextImager/Relationship/SentenceInParagraph"
		),
		TokenInParagraph(
				"tokenInParagraph", "http://hucompute.org/TextImager/Relationship/TokenInParagraph"
		),
		TokenInSentence(
				"tokenInSentence", "http://hucompute.org/TextImager/Relationship/TokenInSentence"
		),
		TokenHasLemma(
				"tokenHasLemma", "http://hucompute.org/TextImager/Relationship/TokenHasLemma"
		),
		TokenAtPos(
				"tokenAtPos", "http://hucompute.org/TextImager/Relationship/TokenAtPos"
		),
		NextParagraph(
				"nextParagraph", "http://hucompute.org/TextImager/Relationship/NextParagraph"
		),
		NextSentence(
				"nextSentence", "http://hucompute.org/TextImager/Relationship/NextSentence"
		),
		NextToken(
				"nextToken", "http://hucompute.org/TextImager/Relationship/NextToken"
		);

		protected String name;
		protected String url;

		Relationship(String name, String url)
		{
			this.name = name;
			this.url = url;
		}

		public String prefix()
		{
			return "PREFIX " + this.name + ": <" + this.url + ">";
		}

		@Override
		public String toString()
		{
			return this.name;
		}
	}

	// Prepare static inserts for all queries.
	protected static Map<String, String> staticValueMap;

	static
	{
		staticValueMap = new HashMap<>();

		// Models
		staticValueMap.put(
				"DocumentPrefix",
				Model.Document.prefix()
		);
		staticValueMap.put(
				"Document",
				Model.Document.toString()
		);
		staticValueMap.put(
				"ParagraphPrefix",
				Model.Paragraph.prefix()
		);
		staticValueMap.put(
				"Paragraph",
				Model.Paragraph.toString()
		);
		staticValueMap.put(
				"SentencePrefix",
				Model.Sentence.prefix()
		);
		staticValueMap.put(
				"Sentence",
				Model.Sentence.toString()
		);
		staticValueMap.put(
				"TokenPrefix",
				Model.Token.prefix()
		);
		staticValueMap.put(
				"Token",
				Model.Token.toString()
		);
		staticValueMap.put(
				"LemmaPrefix",
				Model.Lemma.prefix()
		);
		staticValueMap.put(
				"Lemma",
				Model.Lemma.toString()
		);
		staticValueMap.put(
				"PosPrefix",
				Model.Pos.prefix()
		);
		staticValueMap.put(
				"Pos",
				Model.Pos.toString()
		);

		// Relationships
		staticValueMap.put(
				"DocumentHasParagraphPrefix",
				Relationship.DocumentHasParagraph.prefix()
		);
		staticValueMap.put(
				"DocumentHasParagraph",
				Relationship.DocumentHasParagraph.toString()
		);
		staticValueMap.put(
				"DocumentHasSentencePrefix",
				Relationship.DocumentHasSentence.prefix()
		);
		staticValueMap.put(
				"DocumentHasSentence",
				Relationship.DocumentHasSentence.toString()
		);
		staticValueMap.put(
				"DocumentHasTokenPrefix",
				Relationship.DocumentHasToken.prefix()
		);
		staticValueMap.put(
				"DocumentHasToken",
				Relationship.DocumentHasToken.toString()
		);
		staticValueMap.put(
				"DocumentHasLemmaPrefix",
				Relationship.DocumentHasLemma.prefix()
		);
		staticValueMap.put(
				"DocumentHasLemma",
				Relationship.DocumentHasLemma.toString()
		);
		staticValueMap.put(
				"SentenceInParagraphPrefix",
				Relationship.SentenceInParagraph.prefix()
		);
		staticValueMap.put(
				"SentenceInParagraph",
				Relationship.SentenceInParagraph.toString()
		);
		staticValueMap.put(
				"TokenInParagraphPrefix",
				Relationship.TokenInParagraph.prefix()
		);
		staticValueMap.put(
				"TokenInParagraph",
				Relationship.TokenInParagraph.toString()
		);
		staticValueMap.put(
				"TokenInSentencePrefix",
				Relationship.TokenInSentence.prefix()
		);
		staticValueMap.put(
				"TokenInSentence",
				Relationship.TokenInSentence.toString()
		);
		staticValueMap.put(
				"TokenHasLemmaPrefix",
				Relationship.TokenHasLemma.prefix()
		);
		staticValueMap.put(
				"TokenHasLemma",
				Relationship.TokenHasLemma.toString()
		);
		staticValueMap.put(
				"TokenAtPosPrefix",
				Relationship.TokenAtPos.prefix()
		);
		staticValueMap.put(
				"TokenAtPos",
				Relationship.TokenAtPos.toString()
		);
		staticValueMap.put(
				"NextParagraphPrefix",
				Relationship.NextParagraph.prefix()
		);
		staticValueMap.put(
				"NextParagraph",
				Relationship.NextParagraph.toString()
		);
		staticValueMap.put(
				"NextSentencePrefix",
				Relationship.NextSentence.prefix()
		);
		staticValueMap.put(
				"NextSentence",
				Relationship.NextSentence.toString()
		);
		staticValueMap.put(
				"NextTokenPrefix",
				Relationship.NextToken.prefix()
		);
		staticValueMap.put(
				"NextToken",
				Relationship.NextToken.toString()
		);
	}

	/**
	 * Small utility for parsing values to fit in url schemes.
	 * Use this whenever a value is used as part of an identifier.
	 * Escapes ".", because Blazegraph can't handle dots in identifiers.
	 * E.g. documentPrefix:parseValue(documentId)
	 * or   posPrefix:parseValue(posValue).
	 */
	protected static String parseValue(String value)
	{
		try
		{
			return URLEncoder.encode(value, "UTF-8")
					.replace(".", "\\.");

			// UTF-8 is supported. Exception will not occur.
		} catch (UnsupportedEncodingException e)
		{
			return null;
		}
	}

	public BlazegraphQueryHandler(String rootEndpoint)
	{
		this.rootEndpoint = rootEndpoint;
	}

	/**
	 * @param query The query to send to the server.
	 * @return a Connection to the SparQL API.
	 */
	protected Connection queryConnection(String query)
	{
		try
		{
			String encodedQuery = URLEncoder.encode(query, "UTF-8");
			String url = this.rootEndpoint + "/bigdata/sparql"
					+ "?query=" + encodedQuery
					+ "&format=json";

			logger.log(Level.FINE, url);

			return Jsoup.connect(url)
					.timeout(0)
					.maxBodySize(0);

			// UTF-8 is supported. Exception will not occur.
		} catch (UnsupportedEncodingException ignored)
		{
			return null;
		}
	}

	/**
	 * Creates a connection with a body.
	 * This only has an effect, if post() or put() are used on the connection.
	 *
	 * @param body The request body.
	 * @return a Connection to the SparQL API.
	 */
	protected Connection postConnection(String body)
	{
		String url = this.rootEndpoint + "/bigdata/sparql?format=json";

		return Jsoup.connect(url)
				.requestBody(body)
				.header("Content-Type", "application/sparql-update;charset=UTF-8")
				.timeout(0)
				.maxBodySize(0);
	}

	protected JSONObject getResult(Document document)
	{
		return new JSONObject(document.body()).getJSONObject("results");
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.Blazegraph;
	}

	@Override
	public void setUpDatabase() throws IOException
	{

	}

	@Override
	public void openDatabase() throws IOException
	{

	}

	@Override
	public void clearDatabase() throws IOException
	{
		this.postConnection("CLEAR ALL").post();
	}

	@Override
	public String storeJCasDocument(JCas document) throws QHException
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "INSERT DATA {\n"
				+ "${Document}:${DocumentId} ${Document}:text     \"\"\"${DocumentText}\"\"\" ;\n"
				+ "                          ${Document}:language \"${DocumentLanguage}\" .\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", parseValue(documentId));
		valueMap.put("DocumentText", document.getDocumentText());
		valueMap.put("DocumentLanguage", document.getDocumentLanguage());
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		logger.log(Level.FINE, query);

		try
		{
			this.postConnection(query).post();
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return documentId;
	}

	@Override
	public String storeParagraph(Paragraph paragraph, String documentId, String previousParagraphId)
	{
		final String paragraphId = UUID.randomUUID().toString();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${ParagraphPrefix}\n"
				+ "${DocumentHasParagraphPrefix}\n"
				+ "${NextParagraphPrefix}\n"
				+ "INSERT DATA {\n"
				+ "  ${Paragraph}:${ParagraphId} ${Paragraph}:begin       ${Begin} ;\n"
				+ "                              ${Paragraph}:end         ${End} .\n"
				+ "  ${Document}:${DocumentId}   ${DocumentHasParagraph}: ${Paragraph}:${ParagraphId} .\n"
				+ ((previousParagraphId == null) ? "" : "  ${Paragraph}:${PreviousParagraphId}   ${NextParagraph}: ${Paragraph}:${ParagraphId} .\n")
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", parseValue(documentId));
		valueMap.put("ParagraphId", parseValue(paragraphId));
		if (previousParagraphId != null)
		{
			valueMap.put("PreviousParagraphId", parseValue(previousParagraphId));
		}
		valueMap.put("Begin", String.valueOf(paragraph.getBegin()));
		valueMap.put("End", String.valueOf(paragraph.getEnd()));
		final StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		logger.log(Level.FINE, query);

		try
		{
			this.postConnection(query).post();
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return paragraphId;
	}

	@Override
	public String storeSentence(Sentence sentence, String documentId, String paragraphId, String previousSentenceId)
	{
		final String sentenceId = UUID.randomUUID().toString();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${ParagraphPrefix}\n"
				+ "${SentencePrefix}\n"
				+ "${DocumentHasSentencePrefix}\n"
				+ "${SentenceInParagraphPrefix}\n"
				+ "${NextSentencePrefix}\n"
				+ "INSERT DATA {\n"
				+ "  ${Sentence}:${SentenceId}   ${Sentence}:begin       ${Begin} ;\n"
				+ "                              ${Sentence}:end         ${End} .\n"
				+ "  ${Document}:${DocumentId}   ${DocumentHasSentence}: ${Sentence}:${SentenceId} .\n"
				+ "  ${Sentence}:${SentenceId}   ${SentenceInParagraph}: ${Paragraph}:${ParagraphId} .\n"
				+ ((previousSentenceId == null) ? "" : "  ${Sentence}:${PreviousSentenceId}   ${NextSentence}: ${Sentence}:${SentenceId} .\n")
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", parseValue(documentId));
		valueMap.put("ParagraphId", parseValue(paragraphId));
		valueMap.put("SentenceId", parseValue(sentenceId));
		if (previousSentenceId != null)
		{
			valueMap.put("PreviousSentenceId", parseValue(previousSentenceId));
		}
		valueMap.put("Begin", String.valueOf(sentence.getBegin()));
		valueMap.put("End", String.valueOf(sentence.getEnd()));
		final StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		logger.log(Level.FINE, query);

		try
		{
			this.postConnection(query).post();
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return sentenceId;
	}

	@Override
	public String storeToken(Token token, String documentId, String paragraphId, String sentenceId, String previousTokenId)
	{
		final String tokenId = UUID.randomUUID().toString();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${ParagraphPrefix}\n"
				+ "${SentencePrefix}\n"
				+ "${TokenPrefix}\n"
				+ "${LemmaPrefix}\n"
				+ "${PosPrefix}\n"
				+ "${TokenInParagraphPrefix}\n"
				+ "${TokenInSentencePrefix}\n"
				+ "${NextTokenPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "${DocumentHasLemmaPrefix}\n"
				+ "${TokenHasLemmaPrefix}\n"
				+ "${TokenAtPosPrefix}\n"
				+ "INSERT DATA {\n"
				+ "  ${Token}:${TokenId}       ${Token}:begin       ${Begin} ;\n"
				+ "                            ${Token}:end         ${End} ;\n"
				+ "                            ${Token}:value       \"${TokenValue}\" ;\n"
				+ "                            ${TokenHasLemma}:    ${Lemma}:${LemmaValue} ;\n"
				+ "                            ${TokenAtPos}:       ${Pos}:${PosValue} ;\n"
				+ "                            ${TokenInParagraph}: ${Paragraph}:${ParagraphId} ;\n"
				+ "                            ${TokenInSentence}:  ${Sentence}:${SentenceId} .\n"
				+ ((previousTokenId == null) ? "" : "  ${Token}:${PreviousTokenId} ${NextToken}: ${Token}:${TokenId} .\n")
				+ "  ${Document}:${DocumentId} ${DocumentHasToken}: ${Token}:${TokenId} ;\n"
				+ "                            ${DocumentHasLemma}: ${Lemma}:${LemmaValue} .\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", parseValue(documentId));
		valueMap.put("ParagraphId", parseValue(paragraphId));
		valueMap.put("SentenceId", parseValue(sentenceId));
		valueMap.put("TokenId", parseValue(tokenId));
		if (previousTokenId != null)
		{
			valueMap.put("PreviousTokenId", parseValue(previousTokenId));
		}
		valueMap.put("Begin", String.valueOf(token.getBegin()));
		valueMap.put("End", String.valueOf(token.getEnd()));
		valueMap.put("TokenValue", token.getLemma().getValue());
		valueMap.put("LemmaValue", parseValue(token.getLemma().getValue()));
		valueMap.put("PosValue", parseValue(token.getPos().getPosValue()));
		final StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		logger.log(Level.FINE, query);

		try
		{
			this.postConnection(query).post();
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return tokenId;
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
	public Set<String> getLemmataForDocument(String documentId) throws DocumentNotFoundException
	{
		return null;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId) throws DocumentNotFoundException, QHException
	{

	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		return 0;
	}

	@Override
	public int countElementsOfType(ElementType type) throws TypeNotCountableException
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfType(String documentId, ElementType type) throws DocumentNotFoundException, TypeNotCountableException
	{
		return 0;
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value) throws TypeNotCountableException, TypeHasNoValueException
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(String documentId, ElementType type, String value) throws DocumentNotFoundException, TypeNotCountableException, TypeHasNoValueException
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
	public Double calculateTTRForDocument(String documentId) throws DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(Collection<String> documentIds)
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
	public Iterable<String> getBiGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments() throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments() throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}
}
