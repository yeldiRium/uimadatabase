package org.hucompute.services.uima.database;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

import org.hucompute.services.uima.database.neo4j.data.Const;
import org.json.JSONObject;

/**
 * QueryHandler abstract superclass. Interprets querys and calls functions.
 * @author Manuel Stoeckel
 * Created on 26.09.2017
 */
public abstract class AbstractQueryHandler implements QueryHandlerInterface {
	
	/**
	 * The first argument is the function name, others are their respective arguments.
	 * The function name is removed from the Collection via .pop() leaving only function-arguments or an empty collection.
	 * <p>In case of a _collection call the entire ArrayDeque will be passed as argument.
	 * Else, function-arguments are filled using .pop().</p>
	 * @param query ArrayDeque (Collection) containing all arguments from a single query.
	 * @return a JSON-Response-Object. 
	 * @throws UnsupportedOperationException when the name of the function is unknown.
	 * @throws NoSuchElementException when the number of arguments is not enough to call the function.
	 * @throws IllegalArgumentException when an argument should be in Const.TYPE, but is not.
	 */
	public JSONObject interpret(ArrayDeque<String> query)
			throws UnsupportedOperationException, NoSuchElementException, IllegalArgumentException{
		JSONObject output = new JSONObject();
		try{
			String func = query.pop();
			switch (func) {
				case "TTR_all": {
					output.put("TTR_all", TTR_all());
					break;
				}
				case "TTR_one": {
					output.put("TTR_one", TTR_one(query.pop()));
					break;
				}
				case "TTR_collection":{
					output.put("TTR_collection", TTR_collection(query));
					break;
				}
				case "count_type": {
					output.put("count_type", count_type(Const.TYPE.valueOf(query.pop().toUpperCase())));
					break;
				}
				case "count_type_in_document": {
					output.put("count_type_in_document", count_type_in_document(
							query.pop(),Const.TYPE.valueOf(query.pop().toUpperCase())));
					break;
				}
				case "count_type_with_value": {
					output.put("count_type_with_value", count_type_with_value(
							Const.TYPE.valueOf(query.pop().toUpperCase()),query.pop()));
					break;
				}
				case "count_type_with_value_in_document": {
					output.put("count_type_with_value_in_document", count_type_with_value_in_document(
							query.pop(),Const.TYPE.valueOf(query.pop().toUpperCase()),query.pop()));
					break;
				}
				case "count_tvd": {
					output.put("count_type_with_value_in_document", count_type_with_value_in_document(
							query.pop(),Const.TYPE.valueOf(query.pop().toUpperCase()),query.pop()));
					break;
				}
				case "get_termFrequency_doubleNorm": {
					output.put("get_termFrequency_doubleNorm", get_termFrequency_doubleNorm(query.pop(),query.pop()));
					break;
				}
				case "get_termFrequency_logNorm": {
					output.put("get_termFrequency_logNorm", get_termFrequency_logNorm(query.pop(),query.pop()));
					break;
				}
				case "get_termFrequencies": {
					output.put("get_termFrequencies", get_termFrequencies(query.pop()));
					break;
				}
				case "get_documentsContaining": {
					output.put("get_documentsContaining", get_documentsContaining(query.pop()));
					break;
				}
				case "get_inverseDocumentFrequency": {
					output.put("get_inverseDocumentFrequency", get_inverseDocumentFrequency(query.pop()));
					break;
				}
				case "get_inverseDocumentFrequencies": {
					output.put("get_inverseDocumentFrequencies", get_inverseDocumentFrequencies(query.pop()));
					break;
				}
				case "get_tfidf": {
					output.put("get_tfidf", get_tfidf(query.pop(),query.pop()));
					break;
				}
				case "get_tfidf_all": {
					output.put("get_tfidf_all", get_tfidf_all(query.pop()));
					break;
				}
				case "get_tfidf_all_all": {
					output.put("get_tfidf_all_all", get_tfidf_all_all());
					break;
				}
				case "get_Lemmata": {
					output.put("get_Lemmata", get_Lemmata(query.pop()));
					break;
				}
				case "get_bi_grams": {
					output.put("get_bi_grams", get_bi_grams(query.pop()));
					break;
				}
				case "get_bi_grams_collection": {
					output.put("get_bi_grams_collection", get_bi_grams_collection(query));
					break;
				}
				case "get_bi_grams_all": {
					output.put("get_bi_grams_all", get_bi_grams_all());
					break;
				}
				case "get_tri_grams": {
					output.put("get_tri_grams", get_tri_grams(query.pop()));
					break;
				}
				case "get_tri_grams_collection": {
					output.put("get_tri_grams_collection", get_tri_grams_collection(query));
					break;
				}
				case "get_tri_grams_all": {
					output.put("get_tri_grams_all", get_tri_grams_all());
					break;
				}
				default:{
					throw new UnsupportedOperationException(func + " is not a valid function name!");
				}
			}
		}
		
		catch (Exception e) {
			if(e.getClass()==UnsupportedOperationException.class){
				e.printStackTrace();
			}else if(e.getClass()==IllegalArgumentException.class){
				e.printStackTrace();
			}else if(e.getClass()==NoSuchElementException.class){
				e.printStackTrace();
			}else{
				System.out.println("Unexpected exception!");
				e.printStackTrace();
			}
		}
		return output;
	}	
	
	/*
	 * All methods that are know to be incompatible with at least one database.
	 * Override in implementation!
	 */
	public ArrayList<String> get_bi_grams(String documentId) throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}
	
	public ArrayList<String> get_bi_grams_all() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}
	
	public ArrayList<String> get_bi_grams_collection(Collection<String> documentIds)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}
	public ArrayList<String> get_tri_grams(String documentId) throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}
	
	public ArrayList<String> get_tri_grams_all() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}
	
	public ArrayList<String> get_tri_grams_collection(Collection<String> documentIds)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}
}
