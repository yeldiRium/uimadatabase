package uimadatabase.dbtest.connection;

import dbtest.connection.Connection;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.QHException;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

public class ConnectionTestCase
{

	class TestConnection extends Connection
	{
		@Override
		protected boolean tryToConnect()
		{
			return true;
		}

		@Override
		public void close()
		{

		}

		@Override
		protected void createQueryHandler()
		{
			this.queryHandler = new QueryHandlerInterface()
			{
				@Override
				public void setUpDatabase()
				{

				}

				@Override
				public void clearDatabase()
				{

				}

				@Override
				public Set<String> getLemmataForDocument(String documentId)
				{
					return null;
				}

				@Override
				public void storeJCasDocument(JCas document)
				{

				}

				@Override
				public void storeParagraph(Paragraph paragraph, JCas document, Paragraph previousParagraph)
				{

				}

				@Override
				public void storeParagraph(Paragraph paragraph, JCas document)
				{

				}

				@Override
				public void storeSentence(Sentence sentence, JCas document, Paragraph paragraph, Sentence previousSentence)
				{

				}

				@Override
				public void storeSentence(Sentence sentence, JCas document, Paragraph paragraph)
				{

				}

				@Override
				public void storeToken(Token token, JCas document, Paragraph paragraph, Sentence sentence, Token previousToken)
				{

				}

				@Override
				public void storeToken(Token token, JCas document, Paragraph paragraph, Sentence sentence)
				{

				}

				@Override
				public void storeJCasDocuments(Iterable<JCas> documents)
				{

				}

				@Override
				public Iterable<String> getDocumentIds()
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
				public int countElementsOfType(ElementType type)
				{
					return 0;
				}

				@Override
				public int countElementsInDocumentOfType(String documentId, ElementType type) throws DocumentNotFoundException
				{
					return 0;
				}

				@Override
				public int countElementsOfTypeWithValue(ElementType type, String value) throws IllegalArgumentException
				{
					return 0;
				}

				@Override
				public int countElementsInDocumentOfTypeWithValue(String documentId, ElementType type, String value) throws DocumentNotFoundException
				{
					return 0;
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
				public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
				{
					return 0;
				}

				@Override
				public double calculateTermFrequencyWithLogNormForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
				{
					return 0;
				}

				@Override
				public Map<String, Double> calculateTermFrequenciesForLemmataInDocument(String documentId) throws DocumentNotFoundException
				{
					return null;
				}

				@Override
				public double calculateInverseDocumentFrequency(String lemma)
				{
					return 0;
				}

				@Override
				public Map<String, Double> calculateInverseDocumentFrequenciesForLemmataInDocument(String documentId) throws DocumentNotFoundException
				{
					return null;
				}

				@Override
				public double calculateTFIDFForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
				{
					return 0;
				}

				@Override
				public Map<String, Double> calculateTFIDFForLemmataInDocument(String documentId) throws DocumentNotFoundException
				{
					return null;
				}

				@Override
				public Map<String, Map<String, Double>> calculateTFIDFForLemmataInAllDocuments()
				{
					return null;
				}

				@Override
				public Iterable<String> getBiGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
				{
					return null;
				}

				@Override
				public Iterable<String> getBiGramsFromAllDocuments() throws UnsupportedOperationException, DocumentNotFoundException
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
				public Iterable<String> getTriGramsFromAllDocuments() throws UnsupportedOperationException, DocumentNotFoundException
				{
					return null;
				}

				@Override
				public Iterable<String> getTriGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
				{
					return null;
				}
			};
		}
	}

	@Test
	void Given_ConnectionWithMockedTryToConnectMethod_When_CallingEstablish_Then_CallsTryToConnectUntilItReturnsTrue()
	{
		TestConnection connection = Mockito.mock(TestConnection.class);
		doCallRealMethod().when(connection).establish();
		doCallRealMethod().when(connection).isEstablished();
		// Test that tryToConnect is called four times, the fourth time succeeding.
		when(connection.tryToConnect()).thenReturn(false, false, false, true);

		connection.establish();

		verify(connection, times(4)).tryToConnect();
		assertEquals(true, connection.isEstablished());
	}

	@Test
	void Given_Connection_When_Established_Then_QueryHandlerShouldBeGettable()
	{
		TestConnection connection = new TestConnection();
		connection.establish();

		assertNotNull(connection.getQueryHandler());
	}
}
