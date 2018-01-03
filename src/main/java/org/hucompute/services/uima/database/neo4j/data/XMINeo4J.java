package org.hucompute.services.uima.database.neo4j.data;

import org.hucompute.services.uima.database.neo4j.impl.MDB_Neo4J_Impl;
import spark.servlet.SparkApplication;

/**
 * Created by abrami on 17.02.17.
 */
public class XMINeo4J implements SparkApplication
{

	@Override
	public void init()
	{

		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl("/home/ahemati/workspace/XMI4Neo4J/conf.conf");

	}

	@Override
	public void destroy()
	{

	}
}
