package org.hucompute.services.uima.eval.database.abstraction;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;

import javax.naming.OperationNotSupportedException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static java.lang.Double.isNaN;

public abstract class AbstractQueryHandler implements QueryHandlerInterface
{
	protected static final Logger logger =
			Logger.getLogger(AbstractQueryHandler.class.getName());

	@Override
	public void storeDocumentHierarchy(JCas document) throws QHException
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		try
		{
			this.storeJCasDocument(document);
		} catch (QHException e)
		{
			logger.severe("There was an error when trying to insert "
					+ documentId + ".");
			logger.severe(Arrays.toString(e.getException().getStackTrace()));
			return;
		}

		try
		{
			/*
			 * Store each element of the document that was annotated as a Para-
			 * graph.
			 */
			String previousParagraphId = null;
			for (Paragraph paragraph
					: JCasUtil.select(document, Paragraph.class))
			{
				String paragraphId = this.storeParagraph(
						paragraph, documentId, previousParagraphId
				);
				previousParagraphId = paragraphId;

				/*
				 * Store each element of the document that was annotated as a
				 * Sentence and is contained in the current paragraph.
				 */
				String previousSentenceId = null;
				for (Sentence sentence : JCasUtil.selectCovered(
						document,
						Sentence.class, paragraph
				))
				{
					String sentenceId = this.storeSentence(
							sentence,
							documentId,
							paragraphId,
							previousSentenceId
					);
					previousSentenceId = sentenceId;


					/*
					 * Store each element of the document that was annotated as
					 * a Token and is contained in the current sentence.
					 */
					String previousTokenId = null;
					for (Token token : JCasUtil.selectCovered(
							document, Token.class, sentence
					))
					{
						previousTokenId = this.storeToken(
								token,
								documentId,
								paragraphId,
								sentenceId,
								previousTokenId
						);
					}
				}
			}
		} catch (UnsupportedOperationException ignored)
		{

		}
	}

	/**
	 * Stores multiple Documents at once.
	 * @param documents An iterable object of Documents.
	 * @return The Documents' ids.
	 */
	@Override
	public Iterable<String> storeJCasDocuments(Iterable<JCas> documents)
	{
		List<String> documentIds = new ArrayList<>();
		for (JCas document : documents)
		{
			documentIds.add(this.storeJCasDocument(document));
		}
		return documentIds;
	}

	/**
	 * @param paragraph  The Paragraph.
	 * @param documentId The id of the document in which the paragraph
	 *                   occurs.
	 * @return The Paragraph's id.
	 */
	@Override
	public String storeParagraph(Paragraph paragraph, String documentId)
	{
		return this.storeParagraph(paragraph, documentId, null);
	}

	/**
	 * @param sentence    The Sentence.
	 * @param documentId  The id of the document in which the paragraph
	 *                    occurs.
	 * @param paragraphId The id of the Paragraph in which the Sentence occurs.
	 * @return The Sentence's id.
	 */
	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId
	)
	{
		return this.storeSentence(
				sentence, documentId, paragraphId, null
		);
	}

	/**
	 * @param token       The Token.
	 * @param documentId  The id of the document in which the paragraph
	 *                    occurs.
	 * @param paragraphId The id of the Paragraph in which the Sentence
	 *                    occurs.
	 * @param sentenceId  The id of the Sentence in which the Token occurs.
	 * @return The Token's id.
	 */
	@Override
	public String storeToken(
			Token token,
			String documentId,
			String paragraphId,
			String sentenceId
	)
	{
		return this.storeToken(
				token, documentId, paragraphId, sentenceId, null
		);
	}

	/**
	 * Throws an TypeHasNoValueException, if the given type has no value field.
	 * I.e. documents don't have a value, but lemmata do.
	 *
	 * @param type
	 */
	protected void checkTypeHasValueField(ElementType type)
			throws TypeHasNoValueException
	{
		if (type == ElementType.Document || type == ElementType.Paragraph
				|| type == ElementType.Sentence)
		{
			throw new TypeHasNoValueException();
		}
	}

	@Override
	public double calculateInverseDocumentFrequency(String lemma)
			throws OperationNotSupportedException
	{
		try
		{
			double result = countElementsOfType(ElementType.Document) /
					(double) countDocumentsContainingLemma(lemma);
			if (result == 0 || isNaN(result))
			{
				return 0;
			} else
			{
				return Math.log(result);
			}
		} catch (TypeNotCountableException e)
		{
			throw new OperationNotSupportedException();
		}
	}

	@Override
	public Map<String, Double>
	calculateInverseDocumentFrequenciesForLemmataInDocument(String documentId)
			throws DocumentNotFoundException, OperationNotSupportedException
	{
		try
		{
			double docCount = (double) countElementsOfType(ElementType.Document);
			Map<String, Double> inverseDocumentFrequencies =
					new ConcurrentHashMap<>();
			Set<String> lemmata = getLemmataForDocument(documentId);

			lemmata.stream().forEach(e -> {
				inverseDocumentFrequencies.put(
						e.replaceAll("\"", ""),
						Math.log(docCount /
								((double) this.countDocumentsContainingLemma(e))
						)
				);
			});
			return inverseDocumentFrequencies;
		} catch (TypeNotCountableException e)
		{
			throw new OperationNotSupportedException();
		}
	}

	@Override
	public double calculateTFIDFForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException, OperationNotSupportedException
	{
		try
		{
			return this.calculateTermFrequencyWithLogNormForLemmaInDocument(
					lemma, documentId
			) * this.calculateInverseDocumentFrequency(lemma);
		} catch (OperationNotSupportedException e)
		{
			throw new OperationNotSupportedException();
		}
	}

	@Override
	public Map<String, Double> calculateTFIDFForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException, OperationNotSupportedException
	{
		Map<String, Double> tf =
				calculateTermFrequenciesForLemmataInDocument(documentId);
		Map<String, Double> idf =
				calculateInverseDocumentFrequenciesForLemmataInDocument(
						documentId
				);
		Map<String, Double> concurrentIdf = new ConcurrentHashMap<>(idf);
		Map<String, Double> tfidf = new ConcurrentHashMap<>();
		tf.entrySet().parallelStream().forEach(
				e -> {
					if (concurrentIdf.containsKey(e.getKey()))
					{
						tfidf.put(
								e.getKey(),
								e.getValue() * concurrentIdf.get(e.getKey())
						);
					}
				}
		);
		return tfidf;
	}

	@Override
	public Map<String, Map<String, Double>>
	calculateTFIDFForLemmataInAllDocuments()
			throws OperationNotSupportedException
	{
		HashMap<String, Map<String, Double>> tfidfs = new HashMap<>();
		for (String documentId : this.getDocumentIds())
		{
			try
			{
				tfidfs.put(
						documentId,
						this.calculateTFIDFForLemmataInDocument(documentId)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId \"" + documentId + "\" could " +
						"not be found in the database, although it was " +
						"there just a moment ago. Please check for " +
						"concurrent access.");
			}
		}
		;
		return tfidfs;
	}

	@Override
	public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException
	{
		Map<String, Integer> rtf = this.calculateRawTermFrequenciesInDocument(
				documentId
		);

		if (rtf.isEmpty())
		{
			return 0.0;
		}

		return 0.5 + 0.5 * (
				((double) rtf.getOrDefault(lemma, 0)) /
						((double) Collections.max(rtf.values()))
		);
	}

	@Override
	public double calculateTermFrequencyWithLogNormForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException
	{
		Integer lemmaCount =
				this.calculateRawTermFrequencyForLemmaInDocument(
						lemma,
						documentId
				);

		if (lemmaCount == 0)
		{
			return 1;
		} else
		{
			return 1 + Math.log(lemmaCount);
		}
	}

	@Override
	public Map<String, Double> calculateTermFrequenciesForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		Map<String, Integer> rawTermFrequencies =
				this.calculateRawTermFrequenciesInDocument(documentId);
		Map<String, Double> termFrequencies = new ConcurrentHashMap<>();

		if (rawTermFrequencies.isEmpty())
		{
			return termFrequencies;
		}

		double max = Collections.max(rawTermFrequencies.values());
		rawTermFrequencies.entrySet().parallelStream().forEach(e -> {
			termFrequencies.put(
					e.getKey(),
					0.5 + 0.5 * (e.getValue() / max)
			);
		});
		return termFrequencies;
	}
}
