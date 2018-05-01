package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.google.common.collect.Maps;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
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
	 * Small utility for parsing values to fit in url schemes.
	 * Use this whenever a value is used as part of an identifier.
	 * E.g. documentPrefix:parseValue(documentId)
	 * or   posPrefix:parseValue(posValue).
	 * <p>
	 * Escapes ".", because Blazegraph can't handle dots in identifiers.
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
		valueMap.put("DocumentId", parseValue(documentId));
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
		valueMap.put("DocumentId", parseValue(documentId));
		valueMap.put("ParagraphId", parseValue(paragraphId));
		if (previousParagraphId != null)
		{
			valueMap.put("PreviousParagraphId", parseValue(previousParagraphId));
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
		valueMap.put("DocumentId", parseValue(documentId));
		valueMap.put("ParagraphId", parseValue(paragraphId));
		valueMap.put("SentenceId", parseValue(sentenceId));
		if (previousSentenceId != null)
		{
			valueMap.put("PreviousSentenceId", parseValue(previousSentenceId));
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
		valueMap.put("DocumentId", parseValue(documentId));
		valueMap.put("ParagraphId", parseValue(paragraphId));
		valueMap.put("SentenceId", parseValue(sentenceId));
		valueMap.put("TokenId", parseValue(tokenId));
		if (previousTokenId != null)
		{
			valueMap.put("PreviousTokenId", parseValue(previousTokenId));
		}
		valueMap.put("BeginValue", String.valueOf(token.getBegin()));
		valueMap.put("EndValue", String.valueOf(token.getEnd()));
		valueMap.put("TokenValue", token.getLemma().getValue());
		valueMap.put("LemmaValue", parseValue(token.getLemma().getValue()));
		valueMap.put("TokenPosValue", token.getPos().getPosValue());
		valueMap.put("PosId", parseValue(token.getPos().getPosValue()));
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
		return null;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId) throws DocumentNotFoundException, QHException
	{
		this.checkIfDocumentExists(documentId);

		final String queryTemplate = "${DocumentPrefix}\n" +
				"${ParagraphPrefix}\n" +
				"${SentencePrefix}\n" +
				"${BeginPrefix}\n" +
				"${EndPrefix}\n" +
				"${ValuePrefix}\n" +
				"${PosValuePrefix}\n" +
				"${TextPrefix}\n" +
				"${LanguagePrefix}\n" +
				"SELECT *\n" +
				"WHERE {\n" +
				"  ${Document}:${DocumentId} ?type ?target .\n" +
				"  OPTIONAL {\n" +
				"    ?target ${Begin}: ?begin .\n" +
				"    ?target ${End}: ?end\n" +
				"  } OPTIONAL {\n" +
				"    ?target ${Value}: ?value\n" +
				"  } OPTIONAL {\n" +
				"    ?target ${PosValue}: ?posValue\n" +
				"  }\n" +
				"}";
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
