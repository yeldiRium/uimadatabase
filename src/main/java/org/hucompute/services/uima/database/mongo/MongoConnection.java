package org.hucompute.services.uima.database.mongo;


import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import java.net.UnknownHostException;

/**
 * A wrapper for a connection to Mongo.
 *
 * @author renaud.richardet@epfl.ch
 */
public class MongoConnection
{

	final public String host, dbName, collectionName;
	//	final public Mongo m;
	final public DB db;
	final public DBCollection coll;

	/**
	 * @param db_connection an array {host, dbName, collectionName, user, pw}. Leave user
	 *                      and pw empty ("") to skip authentication
	 */
	public MongoConnection(String[] db_connection) throws UnknownHostException,
			MongoException
	{
		this(db_connection[0], db_connection[1], db_connection[2], true);
	}

	/**
	 * @param host
	 * @param dbName
	 * @param collectionName
	 * @param safe
	 */
	@SuppressWarnings("deprecation") // TODO replace with MongoClient
	public MongoConnection(String host, String dbName, String collectionName, boolean safe)
			throws UnknownHostException, MongoException
	{


		this.host = host;
		this.dbName = dbName;
		this.collectionName = collectionName;
//		user = db_connection[3];
//		pw = db_connection[4];

//		checkNotNull(host, "host is NULL");
//		checkNotNull(dbName, "dbName is NULL");
//		checkNotNull(collectionName, "collectionName is NULL");
//		checkNotNull(user, "user is NULL");
//		checkNotNull(pw, "pw is NULL");

//		m = new Mongo(host, 27017);
//		if (safe)
//			m.setWriteConcern(WriteConcern.SAFE);
//		m.getDatabaseNames();// to test connection
//		db = m.getDB(dbName);

		MongoClient mongoClient = new MongoClient(host);
		db = mongoClient.getDB(dbName);
//		if (user.length() > 0) {
//			if (!db.authenticate(user, pw.toCharArray())) {
//				throw new MongoException(-1, "cannot login with user " + user);
//			}
//		}
		System.out.println(db.getName());
		coll = db.getCollection(collectionName);
	}

	@Override
	public String toString()
	{
		return "MongoConnection: " + host + ":" + dbName + "::"
				+ collectionName;
	}
}
