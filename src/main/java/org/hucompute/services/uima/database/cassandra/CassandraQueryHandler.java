package org.hucompute.services.uima.database.cassandra;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.hucompute.services.uima.database.AbstractQueryHandler;
import org.hucompute.services.uima.database.RequestHandler;
import org.hucompute.services.uima.database.Const.TYPE;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

/**
 * Here we send queries to Cassandra database, manipulate it and return it.
 * {@link RequestHandler} can be used to run this and have it run
 * against other implementations of {@link AbstractQueryHandler}. functions
 * throwing UnsupportedOperationException have been left unimplemented since
 * querying them in Cassandra is inefficient and unnecessarily complex. Can be
 * handled more easy with other databases.
 * 
 * @author Luis Glaser
 *
 */
public class CassandraQueryHandler extends AbstractQueryHandler {
	private Session session;
	private Cluster cluster;
	private ArrayDeque<String> query;
	
	public CassandraQueryHandler(String[] in) {
		this.query = new ArrayDeque<>();
		this.query.addAll(Arrays.asList(in));
		// Change below to change to corresponding cluster
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		this.session = cluster.connect("textimager");
		this.session.execute("use textimager;");
	}
	
	@Override
	public JSONObject call() throws Exception {
		
		JSONObject output = new JSONObject();
		output = interpret(this.query);
		
		try {
			cluster.close();
		}
		catch (Exception e) {
			System.err.println("Cassandra connection did not properly close.");
		}
		
		return output;
		
		
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments() {
		long start;
		long end;
		Map<String, Double> ttr = new HashMap<>();
		String ttrQuery = "SELECT xmi, value FROM ttr;";
		try {
			start = System.currentTimeMillis();
			ResultSet ttrSet = session.execute(ttrQuery);
			for (Row result : ttrSet) {
				ttr.put(result.getString("xmi"), (double) result.getFloat("value")); 
			}
			end = System.currentTimeMillis();
			System.out.println("Getting all TTR values took "+(end-start)+" ms.");
		}
		catch (Exception e) {
			System.err.println("Getting TTR failed!");
			e.printStackTrace();
		}
		return ttr;
	}

	@Override
	public Map<String, Double> calculateTTRForDocument(String documentId) {
		long start;
		long end;
		Map<String, Double> ttr = new HashMap<>();
		try {
			start = System.currentTimeMillis();
			SimpleStatement stmt = new SimpleStatement("SELECT xmi, value FROM ttr WHERE xmi=?", documentId);
			ResultSet rs = session.execute(stmt);
			for (Row result : rs) {
				ttr.put(result.getString("xmi"), (double) result.getFloat("value"));
			}
			end = System.currentTimeMillis();
			System.out.println("Getting TTR for "+documentId+" took "+(end-start)+" ms.");
		}
		catch (Exception e) {
			System.err.println("Getting TTR failed!");
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(Collection<String> documentIds) {
		long start;
		long end;
		Map<String, Double> ttrCollection = new HashMap<>();
		HashSet<SimpleStatement> batchSet = new HashSet<>();
		int counter = 0;
		try {
			start = System.currentTimeMillis();
			for (String documentID : documentIds) {
				batchSet.add(new SimpleStatement("SELECT xmi, value FROM ttr WHERE xmi=?", documentID));
				counter++;
				if(counter >= 5000) {
					CassandraQueryUtil.batchQueries(session, batchSet, "s");
					counter = 0;
					batchSet.clear();
				}
			}
			// using up left over batched queries.
			CassandraQueryUtil.batchQueries(session, batchSet, "s");
			counter = 0;
			batchSet.clear();
			end = System.currentTimeMillis();
			System.out.println("Getting TTR for "+documentIds.size()+" documents took"+(end-start+" ms."));
		}
		catch (Exception e) {
			System.err.println("Getting TTR failed!");
			e.printStackTrace();
		}
		
		return ttrCollection;
	}

	@Override
	public int countElementsOfType(TYPE type) throws UnsupportedOperationException{
		throw new UnsupportedOperationException();
	}

	@Override
	public int countElementsInDocumentOfType(String documentId, TYPE type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int countElementsOfTypeWithValue(TYPE type, String value) throws IllegalArgumentException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(String documentId, TYPE type, String value)
			throws IllegalArgumentException {
		throw new UnsupportedOperationException();
	}

	@Override
	public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(String lemma, String documentId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public double calculateTermFrequencyWithLogNermForLemmaInDocument(String lemma, String documentId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public HashMap<String, Double> calculateTermFrequenciesForLemmataInDocument(String documentId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int countDocumentsContainingLemma(String lemma) {
		int count = 0;
		long start;
		long end;
		try {
			start = System.currentTimeMillis();
			SimpleStatement getCount = new SimpleStatement("SELECT value FROM lemmaoccurrences WHERE lemma=?", lemma);
			ResultSet rs = session.execute(getCount);
			for (Row row : rs) {
				count = row.getInt("value");
			}
			end = System.currentTimeMillis();
			System.out.println("Getting count of occurencces of "+lemma+" took "+(end-start)+" ms.");
		}
		catch (Exception e) {
			System.err.println("Getting count of occurrences failed!");
			e.printStackTrace();
		}
		return count;
	}

	@Override
	public double calculateInverseDocumentFrequency(String lemma) {
		double idf = new Double(0.000000);
		long start;
		long end;
		try {
			start = System.currentTimeMillis();
			SimpleStatement getIdf = new SimpleStatement("SELECT value FROM idf WHERE lemma=?", lemma);
			ResultSet rs = session.execute(getIdf);
			for (Row row : rs) {
				idf = (double) row.getFloat("value");
			}
			end = System.currentTimeMillis();
			System.out.println("Getting idf for "+lemma+" took "+(end-start)+" ms.");
		}
		catch (Exception e) {
			System.err.println("Getting idf failed!");
			e.printStackTrace();
		}
		return idf;
	}

	@Override
	public HashMap<String, Double> calculateInverseDocumentFrequenciesForLemmataInDocument(String documentId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public double calculateTFIDFForLemmaInDocument(String lemma, String documentId) {
		double tdidf = new Double(0.000000);
		long start;
		long end;
		try {
			start = System.currentTimeMillis();
			SimpleStatement gettdidf = new SimpleStatement("SELECT value FROM tdidf WHERE xmi=? AND lemma=?", documentId, lemma);
			ResultSet rs = session.execute(gettdidf);
			for (Row row : rs) {
				tdidf = (double) row.getFloat("value");
			
			}
			end = System.currentTimeMillis();
			System.out.println("Getting tfidf for "+lemma+" in "+documentId+" took "+(end-start)+" ms.");
		}
		catch (Exception e) {
			System.err.println("Getting idf failed!");
			e.printStackTrace();
		}
		return tdidf;
	}

	@Override
	public HashMap<String, Double> calculateTFIDFForLemmataInDocument(String documentId) {
		HashMap<String, Double> tdidf = new HashMap<>();
		long start;
		long end;
		try {
			start = System.currentTimeMillis();
			SimpleStatement getTfIdf = new SimpleStatement("SELECT lemma, value FROM tfidf WHERE xmi=?", documentId);
			ResultSet rs = session.execute(getTfIdf);
			for (Row result : rs) {
				tdidf.put(result.getString("lemma"), (double) result.getFloat("value"));
			}
			end = System.currentTimeMillis();
			System.out.println("Getting tdfidf for document "+documentId+" took "+(end-start)+" ms.");
		}
		catch (Exception e) {
			System.err.println("Getting tdidf failed!");
			e.printStackTrace();
		}
		return tdidf;
	}

	@Override
	public HashMap<String, HashMap<String, Double>> calculateTFIDFForLemmataInAllDocuments() {
		HashMap<String, HashMap<String, Double>> tdidfs = new HashMap<>();
		long start;
		long end;
		try {
			start = System.currentTimeMillis();
			SimpleStatement getTfIdfs = new SimpleStatement("SELECT * FROM tfidf;");
			ResultSet rs = session.execute(getTfIdfs);
			for (Row result : rs) {
				String docID = result.getString("xmi");
				HashMap<String, Double> tfidf = tdidfs.getOrDefault(docID, new HashMap<>());
				tfidf.put(result.getString("lemma"), (double) result.getFloat("value"));
				tdidfs.put(docID, tfidf);
			}
			end = System.currentTimeMillis();
			System.out.println("Getting tdidf for all documents took "+(end-start)+" ms.");
		}
		catch (Exception e) {
			System.err.println("Getting tdidfs failed!");
			e.printStackTrace();
		}
		System.out.println(tdidfs.size());
		return tdidfs;
	}

	@Override
	public HashSet<String> getLemmataForDocument(String documentId) {
		HashSet<String> vocabulary = new HashSet<>();
		long start;
		long end;
		try {
			start = System.currentTimeMillis();
			SimpleStatement getVoc = new SimpleStatement("SELECT value FROM lemma WHERE xmi=?", documentId);
			ResultSet rs = session.execute(getVoc);
			for (Row result : rs) {
				vocabulary.add(result.getString("value"));
			}
			end = System.currentTimeMillis();
			System.out.println("Getting vocabulary of document "+documentId+" took "+(end-start)+" ms.");
		}
		catch (Exception e) {
			System.err.println("Getting vocabulary for "+documentId+" failed!");
			e.printStackTrace();
		}
		return vocabulary;
	}

	@Override
	public ArrayList<String> getBiGramsFromDocument(String documentId) throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ArrayList<String> getBiGramsFromAllDocuments() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ArrayList<String> getBiGramsFromDocumentsInCollection(Collection<String> documentIds)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

}
