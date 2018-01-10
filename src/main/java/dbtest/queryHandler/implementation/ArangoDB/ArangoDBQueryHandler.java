package dbtest.queryHandler.implementation.ArangoDB;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeDefinition;
import dbtest.queryHandler.AbstractQueryHandler;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.QHException;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;

import java.util.*;

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
		this.arangodb.createDatabase(dbName);
		ArangoDatabase db = this.arangodb.db(dbName);

		Arrays.stream(ElementType.values()).parallel()
				.map(Enum::toString)
				.forEach(db::createCollection);

		Arrays.stream(Relationship.values()).parallel()
				.map(Enum::toString)
				.forEach(db::createCollection);

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

		db.createGraph(
				graphName, edgeDefinitions
		);
		this.graph = db.graph(graphName);
	}

	/**
	 * It is easier to drop and rebuild the database than to delete everything
	 * via query.
	 */
	@Override
	public void clearDatabase()
	{
		this.arangodb.db(dbName).drop();
		this.setUpDatabase();
	}

	@Override
	public void storeJCasDocument(JCas document)
	{

	}

	@Override
	public void storeJCasDocuments(Iterable<JCas> documents)
	{

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
