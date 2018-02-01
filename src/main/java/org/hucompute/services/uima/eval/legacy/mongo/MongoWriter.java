package org.hucompute.services.uima.eval.legacy.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.json.JsonCasSerializerModified;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.eval.legacy.AbstractWriter;

import java.io.IOException;
import java.io.StringWriter;

public class MongoWriter extends AbstractWriter
{

//	@ConfigurationParameter(name = MongoCollectionReader.PARAM_DB_CONNECTION, mandatory = true, //
//			description = "host, dbname, collectionname, user, pw")
//	private String[] db_connection;

	public static final String PARAM_DB_HOST = "mongo_connection_host";
	@ConfigurationParameter(name = PARAM_DB_HOST)
	protected String db_connection_host;

	public static final String PARAM_DB_DBNAME = "mongo_connection_dbname";
	@ConfigurationParameter(name = PARAM_DB_DBNAME)
	protected String db_connection_dbname;

	public static final String PARAM_DB_COLLECTIONNAME = "mongo_connection_collectionname";
	@ConfigurationParameter(name = PARAM_DB_COLLECTIONNAME)
	protected String db_connection_collectionname;

	public static final String PARAM_CONNECTION_SAFE_MODE = "safeMode";
	@ConfigurationParameter(name = PARAM_CONNECTION_SAFE_MODE, defaultValue = "true", //
			description = "Mongo's WriteConcern SAFE(true) or NORMAL(false)")
	private boolean safeMode;

	private DBCollection coll;

	private JsonCasSerializerModified xcs;

	@Override
	public void initialize(UimaContext context)
			throws ResourceInitializationException
	{
		super.initialize(context);
		xcs = new JsonCasSerializerModified();
		xcs.setOmit0Values(true);
		xcs.setJsonContext(org.apache.uima.json.JsonCasSerializerModified.JsonContextFormat.omitContext);
//		try {
//			MongoClient mongoClient = new MongoClient( "localhost" );
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		try
		{
			MongoConnection conn = new MongoConnection(db_connection_host, db_connection_dbname, db_connection_collectionname, safeMode);
			coll = conn.coll;
		} catch (IOException e)
		{
			throw new ResourceInitializationException(e);
		}
	}

	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException
	{
		resumeWatch();
		try
		{

			StringWriter sw = new StringWriter();
			xcs.serialize(jCas.getCas(), sw);

			DBObject doc = (DBObject) JSON.parse(
					sw.toString()
							.replaceAll("\"begin\"", "\"b\"")
							.replaceAll("\"end\"", "\"e\"")
							.replaceAll("\"xmi:id\"", "\"xid\"")
							.replaceAll("\"sofa\"", "\"s\"")
			);

			coll.insert(doc);
		} catch (Throwable t)
		{
			throw new AnalysisEngineProcessException(t);
		}
		suspendWatch();
//		log();
	}
}