package org.hucompute.services.uima.database.mongo;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class MongoDBQueries {
	static DB db;
	static DBCollection coll;

	public static void main(String[] args) throws IOException {
		String[] db_connection = new String[]{"localhost","test_with_index","wikipedia","",""};
		String host = db_connection[0];
		String dbName = db_connection[1];
		String collectionName = db_connection[2];
		String user = db_connection[3];
		String pw = db_connection[4];

		MongoClient mongoClient = new MongoClient( host );
		db = mongoClient.getDB(dbName);
		if (user.length() > 0) {
			if (/*!db.authenticate(user, pw.toCharArray()) TODO:fix this*/false) {
				throw new MongoException(-1, "cannot login with user " + user);
			}
		}
		
		System.out.println(db.getName());
		coll = db.getCollection(collectionName);
		
		getSumPos();
	}
	
	public static void getSumPos() throws IOException{
		long total = 0;
		int epochs = 10;
		for(int i = 0; i<epochs;i++){
			long start = System.currentTimeMillis();
			
			
			List<DBObject>pipeline = new ArrayList<>();
			//		pipeline.add((DBObject)JSON.parse("{$match:{'_id':'100021'}}"));
			pipeline.add((DBObject)JSON.parse(FileUtils.readFileToString(new File("src/main/java/dbtest/mongo/sumPos.query"))));
			System.out.println(coll.aggregate(pipeline).results().iterator().next());
			System.out.println(System.currentTimeMillis()-start);
			total +=System.currentTimeMillis()-start;
		}
		System.out.println(total/epochs);
	}

}
