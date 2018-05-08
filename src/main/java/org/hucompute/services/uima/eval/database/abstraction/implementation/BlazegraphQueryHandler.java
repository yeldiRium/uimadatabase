package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.google.common.collect.Maps;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.driver.internal.util.Iterables;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

		public String url()
		{
			return this.url;
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

		public String url()
		{
			return this.url;
		}
	}

	public enum Property
	{
		Begin(
				"begin", "http://hucompute.org/TextImager/Property/Begin"
		),
		End(
				"end", "http://hucompute.org/TextImager/Property/End"
		),
		Value(
				"value", "http://hucompute.org/TextImager/Property/Value"
		),
		PosValue(
				"posValue", "http://hucompute.org/TextImager/Property/PosValue"
		),
		Text(
				"text", "http://hucompute.org/TextImager/Property/Text"
		),
		Language(
				"language", "http://hucompute.org/TextImager/Property/Language"
		);

		protected String name;
		protected String url;

		Property(String name, String url)
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

		public String url()
		{
			return this.url;
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

		staticValueMap.put(
				"BeginPrefix",
				Property.Begin.prefix()
		);
		staticValueMap.put(
				"Begin",
				Property.Begin.toString()
		);
		staticValueMap.put(
				"EndPrefix",
				Property.End.prefix()
		);
		staticValueMap.put(
				"End",
				Property.End.toString()
		);
		staticValueMap.put(
				"ValuePrefix",
				Property.Value.prefix()
		);
		staticValueMap.put(
				"Value",
				Property.Value.toString()
		);
		staticValueMap.put(
				"PosValuePrefix",
				Property.PosValue.prefix()
		);
		staticValueMap.put(
				"PosValue",
				Property.PosValue.toString()
		);
		staticValueMap.put(
				"TextPrefix",
				Property.Text.prefix()
		);
		staticValueMap.put(
				"Text",
				Property.Text.toString()
		);
		staticValueMap.put(
				"LanguagePrefix",
				Property.Language.prefix()
		);
		staticValueMap.put(
				"Language",
				Property.Language.toString()
		);
	}

	/**
	 * Escapes dots per default.
	 */
	protected static String encodeId(String value)
	{
		return encodeId(value, true);
	}

	/**
	 * Small utility for parsing values to fit in url schemes.
	 * Use this whenever a value is used as part of an identifier.
	 * E.g. documentPrefix:encodeId(documentId)
	 * or   posPrefix:encodeId(posValue).
	 * <p>
	 * Escapes "." if told to, because Blazegraph can't handle dots in identifi-
	 * ers.
	 */
	protected static String encodeId(String value, Boolean escapeDot)
	{
		try
		{
			String encoded = URLEncoder.encode(value, "UTF-8");
			if (escapeDot)
			{
				return encoded
						.replace(".", "\\.")
						.replace("*", "\\*");
			} else
			{
				return encoded;
			}

			// UTF-8 is supported. Exception will not occur.
		} catch (UnsupportedEncodingException e)
		{
			return null;
		}
	}

	protected static String decodeId(String value)
	{
		try
		{
			return URLDecoder.decode(
					value.replace("\\.", ".")
						.replace("\\*", "*"),
					"UTF-8"
			);

			// UTF-8 is supported. Exception will not occur.
		} catch (UnsupportedEncodingException e)
		{
			return null;
		}
	}

	protected static String encodeValue(String value)
	{
		return value.replace("\"", "\\\"");
	}

	protected static String decodeValue(String value)
	{
		return value.replace("\\\"", "\"");
	}

	public BlazegraphQueryHandler(String rootEndpoint)
	{
		this.rootEndpoint = rootEndpoint;
	}

	/**
	 * @param query The query to send to the server.
	 * @return the server's response.
	 */
	protected JSONObject sendGetRequest(String query)
	{
		try
		{
			String encodedQuery = URLEncoder.encode(query, "UTF-8");
			String url = this.rootEndpoint + "/bigdata/sparql"
					+ "?query=" + encodedQuery
					+ "&format=json";

			return new JSONObject(
					Request.Get(url)
							.addHeader("Accept", "application/sparql-results+json")
							.execute()
							.returnContent()
							.asString()
			);
		} catch (HttpResponseException e)
		{
			logger.warning(query);
			e.printStackTrace();
			throw new QHException(e);
		} catch (IOException e)
		{
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates a connection with a body.
	 * This usually does not return relevant info. Also the result is seemingly
	 * always html and can't be parsed into json.
	 *
	 * @param body The request body.
	 * @return the server's response.
	 */
	protected String sendPostRequest(String body)
	{
		String url = this.rootEndpoint + "/bigdata/sparql?format=json";

		try
		{
			return Request.Post(url)
					.bodyString(body, ContentType.create("application/sparql-update", "UTF-8"))
					.addHeader("Accept", "application/sparql-results+json")
					.execute()
					.returnContent()
					.asString();
		} catch (HttpResponseException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		} catch (IOException e)
		{
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Extracts the actual response content from a blazegraph rest response ob-
	 * ject.
	 *
	 * @param response
	 * @return
	 */
	protected JSONArray extractResults(JSONObject response)
	{
		return response
				.getJSONObject("results")
				.getJSONArray("bindings");
	}

	/**
	 * Extracts the id of an object from its uri.
	 *
	 * @param uri
	 * @return
	 */
	protected static String getIdFromUrl(String uri)
	{
		String[] parts = uri.split("#");
		if (parts.length > 1)
		{
			return parts[1];
		} else
		{
			return "";
		}
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
		this.sendPostRequest("CLEAR ALL");
	}

	@Override
	public String storeJCasDocument(JCas document) throws QHException
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${TextPrefix}\n"
				+ "${LanguagePrefix}\n"
				+ "INSERT DATA {\n"
				+ "${Document}:${DocumentId} ${Text}:     \"\"\"${DocumentText}\"\"\" ;\n"
				+ "                          ${Language}: \"${DocumentLanguage}\" .\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", encodeId(documentId));
		valueMap.put("DocumentText", document.getDocumentText());
		valueMap.put("DocumentLanguage", document.getDocumentLanguage());
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		this.sendPostRequest(query);
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
				+ "${BeginPrefix}\n"
				+ "${EndPrefix}\n"
				+ "INSERT DATA {\n"
				+ "  ${Paragraph}:${ParagraphId} ${Begin}:       ${BeginValue} ;\n"
				+ "                              ${End}:         ${EndValue} .\n"
				+ "  ${Document}:${DocumentId}   ${DocumentHasParagraph}: ${Paragraph}:${ParagraphId} .\n"
				+ ((previousParagraphId == null) ? "" : "  ${Paragraph}:${PreviousParagraphId}   ${NextParagraph}: ${Paragraph}:${ParagraphId} .\n")
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", encodeId(documentId));
		valueMap.put("ParagraphId", encodeId(paragraphId));
		if (previousParagraphId != null)
		{
			valueMap.put("PreviousParagraphId", encodeId(previousParagraphId));
		}
		valueMap.put("BeginValue", String.valueOf(paragraph.getBegin()));
		valueMap.put("EndValue", String.valueOf(paragraph.getEnd()));
		final StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		this.sendPostRequest(query);
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
				+ "${BeginPrefix}\n"
				+ "${EndPrefix}\n"
				+ "INSERT DATA {\n"
				+ "  ${Sentence}:${SentenceId}   ${Begin}:       ${BeginValue} ;\n"
				+ "                              ${End}:         ${EndValue} .\n"
				+ "  ${Document}:${DocumentId}   ${DocumentHasSentence}: ${Sentence}:${SentenceId} .\n"
				+ "  ${Sentence}:${SentenceId}   ${SentenceInParagraph}: ${Paragraph}:${ParagraphId} .\n"
				+ ((previousSentenceId == null) ? "" : "  ${Sentence}:${PreviousSentenceId}   ${NextSentence}: ${Sentence}:${SentenceId} .\n")
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", encodeId(documentId));
		valueMap.put("ParagraphId", encodeId(paragraphId));
		valueMap.put("SentenceId", encodeId(sentenceId));
		if (previousSentenceId != null)
		{
			valueMap.put("PreviousSentenceId", encodeId(previousSentenceId));
		}
		valueMap.put("BeginValue", String.valueOf(sentence.getBegin()));
		valueMap.put("EndValue", String.valueOf(sentence.getEnd()));
		final StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		this.sendPostRequest(query);
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
				+ "${BeginPrefix}\n"
				+ "${EndPrefix}\n"
				+ "${ValuePrefix}\n"
				+ "${PosValuePrefix}\n"
				+ "INSERT DATA {\n"
				+ "  ${Token}:${TokenId}       ${Begin}:            ${BeginValue} ;\n"
				+ "                            ${End}:              ${EndValue} ;\n"
				+ "                            ${Value}:            \"${TokenValue}\" ;\n"
				+ "                            ${PosValue}:         \"${TokenPosValue}\" ;\n"
				+ "                            ${TokenHasLemma}:    ${Lemma}:${LemmaValue} ;\n"
				+ "                            ${TokenAtPos}:       ${Pos}:${PosId} ;\n"
				+ "                            ${TokenInParagraph}: ${Paragraph}:${ParagraphId} ;\n"
				+ "                            ${TokenInSentence}:  ${Sentence}:${SentenceId} .\n"
				+ ((previousTokenId == null) ? "" : "  ${Token}:${PreviousTokenId} ${NextToken}: ${Token}:${TokenId} .\n")
				+ "  ${Document}:${DocumentId} ${DocumentHasToken}: ${Token}:${TokenId} ;\n"
				+ "                            ${DocumentHasLemma}: ${Lemma}:${LemmaValue} .\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", encodeId(documentId));
		valueMap.put("ParagraphId", encodeId(paragraphId));
		valueMap.put("SentenceId", encodeId(sentenceId));
		valueMap.put("TokenId", encodeId(tokenId));
		if (previousTokenId != null)
		{
			valueMap.put("PreviousTokenId", encodeId(previousTokenId));
		}
		valueMap.put("BeginValue", String.valueOf(token.getBegin()));
		valueMap.put("EndValue", String.valueOf(token.getEnd()));
		valueMap.put("TokenValue", encodeValue(token.getLemma().getValue()));
		valueMap.put("LemmaValue", encodeId(token.getLemma().getValue()));
		valueMap.put("TokenPosValue", encodeValue(token.getPos().getPosValue()));
		valueMap.put("PosId", encodeId(token.getPos().getPosValue()));
		final StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		this.sendPostRequest(query);
		return tokenId;
	}

	@Override
	public void checkIfDocumentExists(String documentId) throws DocumentNotFoundException
	{
		final String queryTemplate = "${DocumentPrefix}" +
				"SELECT (count(*) as ?count)\n" +
				"WHERE {\n" +
				"  ${Document}:${DocumentId} ?y ?z\n" +
				"}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray results = this.extractResults(
				this.sendGetRequest(query)
		);
		String count = results
				.getJSONObject(0)
				.getJSONObject("count")
				.getString("value");

		if (Integer.valueOf(count) == 0)
		{
			throw new DocumentNotFoundException();
		}
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		List<String> documentIds = new ArrayList<>();

		final String queryTemplate = "${DocumentPrefix}" +
				"SELECT distinct ?doc\n" +
				"WHERE {\n" +
				"  ?doc ?y ?z\n" +
				"  FILTER (strstarts(str(?doc), str(${Document}:)))\n" +
				"}";
		StrSubstitutor sub = new StrSubstitutor(staticValueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray results = this.extractResults(
				this.sendGetRequest(query)
		);

		for (int i = 0; i < results.length(); i++)
		{
			JSONObject current = results.getJSONObject(i);
			documentIds.add(
					getIdFromUrl(
							current.getJSONObject("doc").getString("value")
					)
			);
		}

		return documentIds;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		Set<String> lemmata = new ConcurrentHashSet<>();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${DocumentHasLemmaPrefix}\n"
				+ "SELECT ?lemma\n"
				+ "WHERE {\n"
				+ "  ${Document}:${DocumentId} ${DocumentHasLemma}: ?lemma\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), true)
				.forEach(elem -> {
					String lemmaUrl = ((JSONObject) elem)
							.getJSONObject("lemma")
							.getString("value");

					lemmata.add(
							decodeId(getIdFromUrl(lemmaUrl))
					);
				});

		return lemmata;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId) throws DocumentNotFoundException, QHException
	{
		this.checkIfDocumentExists(documentId);

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${ParagraphPrefix}\n"
				+ "${SentencePrefix}\n"
				+ "${BeginPrefix}\n"
				+ "${EndPrefix}\n"
				+ "${ValuePrefix}\n"
				+ "${PosValuePrefix}\n"
				+ "${TextPrefix}\n"
				+ "${LanguagePrefix}\n"
				+ "SELECT *\n"
				+ "WHERE {\n"
				+ "  ${Document}:${DocumentId} ?type ?target .\n"
				+ "  OPTIONAL {\n"
				+ "    ?target ${Begin}: ?begin .\n"
				+ "    ?target ${End}: ?end\n"
				+ "  } OPTIONAL {\n"
				+ "    ?target ${Value}: ?value\n"
				+ "  } OPTIONAL {\n"
				+ "    ?target ${PosValue}: ?posValue\n"
				+ "  }\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);
		try
		{
			DocumentMetaData meta = DocumentMetaData.create(aCAS);
			meta.setDocumentId(documentId);
		} catch (CASException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		StreamSupport.stream(result.spliterator(), true)
				.collect(Collectors.groupingByConcurrent(
						obj -> ((JSONObject) obj).getJSONObject("type").getString("value")
				))
				.entrySet().parallelStream()
				.forEach(entrySet -> {
					try
					{
						String key = entrySet.getKey();
						if (key.equals(Relationship.DocumentHasToken.url()))
						{
							for (Object obj : entrySet.getValue())
							{
								JSONObject token = (JSONObject) obj;
								Token xmiToken = null;
								xmiToken = new Token(
										aCAS.getJCas(),
										Integer.valueOf(token.getJSONObject("begin").getString("value")),
										Integer.valueOf(token.getJSONObject("end").getString("value"))
								);

								Lemma lemma = new Lemma(aCAS.getJCas(), xmiToken.getBegin(), xmiToken.getEnd());
								lemma.setValue(token.getJSONObject("value").getString("value"));
								lemma.addToIndexes();
								xmiToken.setLemma(lemma);

								POS pos = new POS(aCAS.getJCas(), xmiToken.getBegin(), xmiToken.getEnd());
								pos.setPosValue(token.getJSONObject("posValue").getString("value"));
								pos.addToIndexes();
								xmiToken.setPos(pos);

								xmiToken.addToIndexes();
							}
						} else if (key.equals(Property.Text.url()))
						{
							String text = ((JSONObject)
									entrySet.getValue().get(0))
									.getJSONObject("target")
									.getString("value");
							aCAS.setDocumentText(text);
						} else if (key.equals(Property.Language.url()))
						{
							String language = ((JSONObject)
									entrySet.getValue().get(0))
									.getJSONObject("target")
									.getString("value");
							aCAS.setDocumentLanguage(language);
						}
					} catch (CASException e)
					{
						e.printStackTrace();
						throw new QHException(e);
					}
				});
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		final String queryTemplate = "${DocumentHasLemmaPrefix}\n" +
				"SELECT (count(distinct ?document) as ?count)\n" +
				"WHERE {\n" +
				" ?document ${DocumentHasLemma}: <${LemmaUrl}${LemmaValue}>\n" +
				"}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		// Somehow can't find Lemmata when using prefix. Probably because of the
		// URL encoding.
		valueMap.put("LemmaUrl", Model.Lemma.url());
		valueMap.put("LemmaValue", encodeId(lemma, false));
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		return Integer.valueOf(
				result.getJSONObject(0).getJSONObject("count").getString("value")
		);
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		String queryTemplate = null;
		switch (type)
		{
			case Document:
				return Iterables.count(this.getDocumentIds());
			case Paragraph:
				queryTemplate = "${ParagraphPrefix}\n"
						+ "${DocumentHasParagraphPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ?x ${DocumentHasParagraph}: ?elem\n"
						+ "  FILTER (strstarts(str(?elem), str(${Paragraph}:)))\n"
						+ "}";
				break;
			case Sentence:
				queryTemplate = "${SentencePrefix}\n"
						+ "${DocumentHasSentencePrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ?x ${DocumentHasSentence}: ?elem\n"
						+ "}";
				break;
			case Token:
				queryTemplate = "${TokenPrefix}\n"
						+ "${DocumentHasTokenPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ?x ${DocumentHasToken}: ?elem\n"
						+ "}";
				break;
			case Lemma:
				queryTemplate = "${LemmaPrefix}\n"
						+ "${DocumentHasLemmaPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ?x ${DocumentHasLemma}: ?elem\n"
						+ "}";
				break;
			case Pos:
				queryTemplate = "${PosPrefix}\n"
						+ "${TokenAtPosPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ?x ${TokenAtPos}: ?elem\n"
						+ "}";
				break;
		}
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		return Integer.valueOf(
				result.getJSONObject(0)
						.getJSONObject("count")
						.getString("value")
		);
	}

	@Override
	public int countElementsInDocumentOfType(String documentId, ElementType type) throws DocumentNotFoundException, TypeNotCountableException
	{
		this.checkIfDocumentExists(documentId);

		String queryTemplate = null;
		switch (type)
		{
			case Document:
				return Iterables.count(this.getDocumentIds());
			case Paragraph:
				queryTemplate = "${DocumentPrefix}\n"
						+ "${ParagraphPrefix}\n"
						+ "${DocumentHasParagraphPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ${Document}:${DocumentId} ${DocumentHasParagraph}: ?elem\n"
						+ "}";
				break;
			case Sentence:
				queryTemplate = "${DocumentPrefix}\n"
						+ "${SentencePrefix}\n"
						+ "${DocumentHasSentencePrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ${Document}:${DocumentId} ${DocumentHasSentence}: ?elem\n"
						+ "}";
				break;
			case Token:
				queryTemplate = "${DocumentPrefix}\n"
						+ "${TokenPrefix}\n"
						+ "${DocumentHasTokenPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ${Document}:${DocumentId} ${DocumentHasToken}: ?elem\n"
						+ "}";
				break;
			case Lemma:
				queryTemplate = "${DocumentPrefix}\n"
						+ "${LemmaPrefix}\n"
						+ "${DocumentHasLemmaPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ${Document}:${DocumentId} ${DocumentHasLemma}: ?elem\n"
						+ "}";
				break;
			case Pos:
				queryTemplate = "${DocumentPrefix}\n"
						+ "${PosPrefix}\n"
						+ "${TokenAtPosPrefix}\n"
						+ "${DocumentHasTokenPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ${Document}:${DocumentId} ${DocumentHasToken} ?token ."
						+ "  ?token ${TokenAtPos}: ?elem\n"
						+ "}";
				break;
		}
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		return Integer.valueOf(
				result.getJSONObject(0)
						.getJSONObject("count")
						.getString("value")
		);
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value) throws TypeNotCountableException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		String queryTemplate;
		switch (type)
		{
			case Token:
				queryTemplate = "${TokenPrefix}\n"
						+ "${ValuePrefix}\n"
						+ "${DocumentHasTokenPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ?x ${DocumentHasToken}: ?elem .\n"
						+ "  ?elem ${Value}: \"${ValueString}\""
						+ "}";
				break;
			case Lemma:
				queryTemplate = "${LemmaPrefix}\n"
						+ "${ValuePrefix}\n"
						+ "${DocumentHasLemmaPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ?x ${DocumentHasLemma}: ?elem\n"
						+ "  FILTER strends(str(?elem), \"${IDValueString}\")\n"
						+ "}";
				break;
			case Pos:
				queryTemplate = "${PosPrefix}\n"
						+ "${ValuePrefix}\n"
						+ "${TokenAtPosPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ?x ${TokenAtPos}: ?elem\n"
						+ "  FILTER strends(str(?elem), \"${IDValueString}\")\n"
						+ "}";
				break;
			// cases without value
			default:
				return 0;
		}
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("ValueString", encodeValue(value));
		valueMap.put("IDValueString", encodeId(value, false));
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		return Integer.valueOf(
				result.getJSONObject(0)
						.getJSONObject("count")
						.getString("value")
		);
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(String documentId, ElementType type, String value) throws DocumentNotFoundException, TypeNotCountableException, TypeHasNoValueException
	{
		this.checkIfDocumentExists(documentId);
		this.checkTypeHasValueField(type);

		String queryTemplate;
		switch (type)
		{
			case Token:
				queryTemplate = "${DocumentPrefix}\n"
						+ "${ValuePrefix}\n"
						+ "${DocumentHasTokenPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ${Document}:${DocumentId} ${DocumentHasToken}: ?elem .\n"
						+ "  ?elem ${Value}: \"${ValueString}\""
						+ "}";
				break;
			case Lemma:
				queryTemplate = "${DocumentPrefix}\n"
						+ "${ValuePrefix}\n"
						+ "${DocumentHasLemmaPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ${Document}:${DocumentId} ${DocumentHasLemma}: ?elem\n"
						+ "  FILTER strends(str(?elem), \"${IDValueString}\")\n"
						+ "}";
				break;
			case Pos:
				queryTemplate = "${DocumentPrefix}\n"
						+ "${ValuePrefix}\n"
						+ "${TokenAtPosPrefix}\n"
						+ "SELECT (count(distinct ?elem) as ?count)\n"
						+ "WHERE {\n"
						+ "  ${Document}:${DocumentId} ${DocumentHasToken} ?token ."
						+ "  ?token ${TokenAtPos}: ?elem\n"
						+ "  FILTER strends(str(?elem), \"${IDValueString}\")\n"
						+ "}";
				break;
			// cases without value
			default:
				return 0;
		}
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		valueMap.put("ValueString", encodeValue(value));
		valueMap.put("IDValueString", encodeId(value, false));
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		return Integer.valueOf(
				result.getJSONObject(0)
						.getJSONObject("count")
						.getString("value")
		);
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		Map<String, Integer> occurenceMap = new ConcurrentHashMap<>();

		final String queryTemplate = "${TokenHasLemmaPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "SELECT ?lemma (count(*) as ?count)\n"
				+ "WHERE {\n"
				+ "  ?document ${DocumentHasToken}: ?token .\n"
				+ "  ?token ${TokenHasLemma}: ?lemma\n"
				+ "}\n"
				+ "GROUP BY ?lemma";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), true)
				.forEach(row -> {
					occurenceMap.put(
							decodeId(getIdFromUrl(
									((JSONObject) row).getJSONObject("lemma").getString("value")
							)),
							Integer.valueOf(((JSONObject) row).getJSONObject("count").getString("value"))
					);
				});

		return occurenceMap;
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		Map<String, Double> ttrMap = new ConcurrentHashMap<>();

		final String queryTemplate = "${DocumentHasLemmaPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "SELECT ?document ((count(?lemma)/(count(?token))) as ?ttr)\n"
				+ "WHERE {\n"
				+ "  { ?document documentHasToken: ?token }\n"
				+ "  UNION\n"
				+ "  { ?document documentHasLemma: ?lemma }\n"
				+ "}\n"
				+ "GROUP BY ?document";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), true)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					ttrMap.put(
							getIdFromUrl(
									row.getJSONObject("document").getString("value")
							),
							Double.valueOf(row.getJSONObject("ttr").getString("value"))
					);
				});

		return ttrMap;
	}

	@Override
	public Double calculateTTRForDocument(String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${DocumentHasLemmaPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "SELECT ((count(?lemma)/(count(?token))) as ?ttr)\n"
				+ "WHERE {\n"
				+ "  { ${Document}:${DocumentId} documentHasToken: ?token }\n"
				+ "  UNION\n"
				+ "  { ${Document}:${DocumentId} documentHasLemma: ?lemma }\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		try
		{
			return Double.valueOf(
					result.getJSONObject(0)
							.getJSONObject("ttr")
							.getString("value")
			);
		} catch (JSONException ignored)
		{
			return 0d;
		}
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(Collection<String> documentIds)
	{
		Map<String, Double> ttrMap = new ConcurrentHashMap<>();

		final StringBuilder builder = new StringBuilder();
		builder.append(
				"${DocumentPrefix}\n"
						+ "${DocumentHasLemmaPrefix}\n"
						+ "${DocumentHasTokenPrefix}\n"
						+ "SELECT ?document ((count(?lemma)/(count(?token))) as ?ttr)\n"
						+ "WHERE {\n"
		);
		for (String documentId : documentIds)
		{
			builder.append("  { \n" +
					"    document:" + documentId + " documentHasToken: ?token .\n" +
					"    ?document documentHasToken: ?token\n" +
					"  }\n" +
					"  UNION\n" +
					"  { \n" +
					"    document:" + documentId + " documentHasLemma: ?lemma .\n" +
					"    ?document documentHasLemma: ?lemma\n" +
					"  }");
		}
		builder.append(
				"}\n"
						+ "GROUP BY ?document\n"
						+ "HAVING (?ttr > 0)"
		);

		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(builder.toString());

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), true)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					ttrMap.put(
							getIdFromUrl(
									row.getJSONObject("document").getString("value")
							),
							Double.valueOf(row.getJSONObject("ttr").getString("value"))
					);
				});

		return ttrMap;
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		Map<String, Integer> frequencyMap = new ConcurrentHashMap<>();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "${TokenHasLemmaPrefix}\n"
				+ "SELECT ?lemma (count(*) as ?occurences)\n"
				+ "WHERE {\n"
				+ "  ${Document}:${DocumentId} ${DocumentHasToken}: ?token .\n"
				+ "  ?token ${TokenHasLemma}: ?lemma\n"
				+ "}\n"
				+ "GROUP BY ?lemma";

		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), true)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					frequencyMap.put(
							decodeId(getIdFromUrl(
									row.getJSONObject("lemma").getString("value")
							)),
							Integer.valueOf(row.getJSONObject("occurences").getString("value"))
					);
				});

		return frequencyMap;
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${LemmaPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "${TokenHasLemmaPrefix}\n"
				+ "SELECT (count(*) as ?count)\n"
				+ "WHERE {\n"
				+ "  ${Document}:${DocumentId} ${DocumentHasToken}: ?token .\n"
				+ "  ?token ${TokenHasLemma}: ${Lemma}:${LemmaValue}\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		valueMap.put("LemmaValue", encodeId(lemma));
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		return Integer.valueOf(
				result.getJSONObject(0)
						.getJSONObject("count")
						.getString("value")
		);
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
	{
		List<String> biGrams = new ArrayList<>();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "${NextTokenPrefix}\n"
				+ "${ValuePrefix}\n"
				+ "SELECT ?v1 ?v2\n"
				+ "WHERE {\n"
				+ "  ${Document}:${DocumentId} ${DocumentHasToken}: ?t1 ."
				+ "  ?t1 ${NextToken}: ?t2 .\n"
				+ "  ?t1 ${Value}: ?v1 .\n"
				+ "  ?t2 ${Value}: ?v2 .\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), false)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					biGrams.add(
							row.getJSONObject("v1").getString("value")
									+ "-"
									+ row.getJSONObject("v2").getString("value")
					);
				});

		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments() throws UnsupportedOperationException
	{
		List<String> biGrams = new ArrayList<>();

		final String queryTemplate = "${NextTokenPrefix}\n"
				+ "${ValuePrefix}\n"
				+ "SELECT ?v1 ?v2\n"
				+ "WHERE {\n"
				+ "  ?t1 ${NextToken}: ?t2 .\n"
				+ "  ?t1 ${Value}: ?v1 .\n"
				+ "  ?t2 ${Value}: ?v2 .\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), false)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					biGrams.add(
							row.getJSONObject("v1").getString("value")
									+ "-"
									+ row.getJSONObject("v2").getString("value")
					);
				});

		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
	{
		List<String> biGrams = new ArrayList<>();

		final StringBuilder builder = new StringBuilder();
		builder.append("${DocumentPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "${NextTokenPrefix}\n"
				+ "${ValuePrefix}\n"
				+ "SELECT ?v1 ?v2\n"
				+ "WHERE {\n");

		for (Iterator<String> iterator = documentIds.iterator(); iterator.hasNext(); )
		{
			String documentId = iterator.next();
			if (iterator.hasNext())
			{
				builder.append("  {"
						+ "    ${Document}:" + documentId + " ${DocumentHasToken}: ?t1"
						+ "  } UNION");
			} else
			{
				builder.append("  {"
						+ "    ${Document}:" + documentId + " ${DocumentHasToken}: ?t1"
						+ "  }");
			}
		}
		builder.append("  ?t1 ${NextToken}: ?t2 .\n"
				+ "  ?t1 ${Value}: ?v1 .\n"
				+ "  ?t2 ${Value}: ?v2 .\n"
				+ "}");
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(builder.toString());

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), false)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					biGrams.add(
							row.getJSONObject("v1").getString("value")
									+ "-"
									+ row.getJSONObject("v2").getString("value")
					);
				});

		return biGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
	{
		List<String> triGrams = new ArrayList<>();

		final String queryTemplate = "${DocumentPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "${NextTokenPrefix}\n"
				+ "${ValuePrefix}\n"
				+ "SELECT ?v1 ?v2 ?v3\n"
				+ "WHERE {\n"
				+ "  ${Document}:${DocumentId} ${DocumentHasToken}: ?t1 ."
				+ "  ?t1 ${NextToken}: ?t2 .\n"
				+ "  ?t2 ${NextToken}: ?t3 .\n"
				+ "  ?t1 ${Value}: ?v1 .\n"
				+ "  ?t2 ${Value}: ?v2 .\n"
				+ "  ?t3 ${Value}: ?v3 .\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		valueMap.put("DocumentId", documentId);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), false)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					triGrams.add(
							row.getJSONObject("v1").getString("value")
									+ "-"
									+ row.getJSONObject("v2").getString("value")
									+ "-"
									+ row.getJSONObject("v3").getString("value")
					);
				});

		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments() throws UnsupportedOperationException
	{
		List<String> triGrams = new ArrayList<>();

		final String queryTemplate = "${NextTokenPrefix}\n"
				+ "${ValuePrefix}\n"
				+ "SELECT ?v1 ?v2 ?v3\n"
				+ "WHERE {\n"
				+ "  ?t1 ${NextToken}: ?t2 .\n"
				+ "  ?t2 ${NextToken}: ?t3 .\n"
				+ "  ?t1 ${Value}: ?v1 .\n"
				+ "  ?t2 ${Value}: ?v2 .\n"
				+ "  ?t3 ${Value}: ?v3 .\n"
				+ "}";
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(queryTemplate);

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), false)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					triGrams.add(
							row.getJSONObject("v1").getString("value")
									+ "-"
									+ row.getJSONObject("v2").getString("value")
									+ "-"
									+ row.getJSONObject("v3").getString("value")
					);
				});

		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
	{
		List<String> triGrams = new ArrayList<>();

		final StringBuilder builder = new StringBuilder();
		builder.append("${DocumentPrefix}\n"
				+ "${DocumentHasTokenPrefix}\n"
				+ "${NextTokenPrefix}\n"
				+ "${ValuePrefix}\n"
				+ "SELECT ?v1 ?v2 ?v3\n"
				+ "WHERE {\n");

		for (Iterator<String> iterator = documentIds.iterator(); iterator.hasNext(); )
		{
			String documentId = iterator.next();
			if (iterator.hasNext())
			{
				builder.append("  {"
						+ "    ${Document}:" + documentId + " ${DocumentHasToken}: ?t1"
						+ "  } UNION");
			} else
			{
				builder.append("  {"
						+ "    ${Document}:" + documentId + " ${DocumentHasToken}: ?t1"
						+ "  }");
			}
		}
		builder.append("  ?t1 ${NextToken}: ?t2 .\n"
				+ "  ?t2 ${NextToken}: ?t3 .\n"
				+ "  ?t1 ${Value}: ?v1 .\n"
				+ "  ?t2 ${Value}: ?v2 .\n"
				+ "  ?t3 ${Value}: ?v3 .\n"
				+ "}");
		final Map<String, String> valueMap = Maps.newHashMap(staticValueMap);
		StrSubstitutor sub = new StrSubstitutor(valueMap);
		final String query = sub.replace(builder.toString());

		JSONArray result = this.extractResults(
				this.sendGetRequest(query)
		);

		StreamSupport.stream(result.spliterator(), false)
				.forEach(obj -> {
					JSONObject row = (JSONObject) obj;
					triGrams.add(
							row.getJSONObject("v1").getString("value")
									+ "-"
									+ row.getJSONObject("v2").getString("value")
									+ "-"
									+ row.getJSONObject("v3").getString("value")
					);
				});

		return triGrams;
	}
}
