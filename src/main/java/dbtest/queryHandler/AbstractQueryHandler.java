package dbtest.queryHandler;

import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.TypeNotCountableException;
import dbtest.queryHandler.exceptions.TypeHasNoValueException;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.jcas.JCas;

import javax.naming.OperationNotSupportedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static java.lang.Double.isNaN;

public abstract class AbstractQueryHandler implements QueryHandlerInterface
{
	protected static final Logger logger =
			Logger.getLogger(AbstractQueryHandler.class.getName());

	/**
	 * @param paragraph The Paragraph.
	 * @param document  The document in which the paragraph occurs.
	 */
	@Override
	public void storeParagraph(Paragraph paragraph, JCas document)
	{
		this.storeParagraph(paragraph, document, null);
	}

	/**
	 * @param sentence  The Sentence.
	 * @param document  The Document in which the entence occurs.
	 * @param paragraph The Paragraph, in which the Sentence occurs.
	 */
	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph
	)
	{
		this.storeSentence(sentence, document, paragraph, null);
	}

	/**
	 * @param token     The Token.
	 * @param document  The id of the document in which the Token occurs.
	 * @param paragraph The paragraph, in which the Token occurs.
	 * @param sentence  The sentence, in which the Token occurs.
	 */
	@Override
	public void storeToken(
			Token token,
			JCas document,
			Paragraph paragraph,
			Sentence sentence
	)
	{
		storeToken(token, document, paragraph, sentence, null);
	}

	/**
	 * Throws an TypeHasNoValueException, if the given type has no value field.
	 * I.e. documents don't have a value, but lemmata do.
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
			} else {
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

			lemmata.parallelStream().forEach(e -> {
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
		Map<String, Double> tfidf = new ConcurrentHashMap<>();
		tf.entrySet().parallelStream().forEach(
				e -> tfidf.put(
						e.getKey(),
						e.getValue() * idf.get(e.getKey())
				)
		);
		return tfidf;
	}

	@Override
	public Map<String, Map<String, Double>>
	calculateTFIDFForLemmataInAllDocuments()
			throws OperationNotSupportedException
	{
		HashMap<String, Map<String, Double>> tfidfs = new HashMap<>();
		for(String documentId : this.getDocumentIds())
		{
			try
			{
				tfidfs.put(
						documentId,
						this.calculateTFIDFForLemmataInDocument(documentId)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId " + documentId + " could " +
						"not be found in the database, although it was " +
						"there just a moment ago. Please check for " +
						"concurrent access.");
			}
		};
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
