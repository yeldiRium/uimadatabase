package org.hucompute.services.uima.eval.database.connection.implementation;

import org.apache.http.client.fluent.Request;
import org.hucompute.services.uima.eval.database.connection.Connection;

import java.io.IOException;

public class SolrConnection extends Connection
{
	protected String rootEndpoint;

	public SolrConnection()
	{
		this.rootEndpoint = "http://" + System.getenv("SOLR_HOST") +
				":" + System.getenv("SOLR_PORT");
	}

	@Override
	protected boolean tryToConnect()
	{
		try
		{
			// Just to test, if the server responds.
			Request.Get(this.rootEndpoint + "/solr/" + System.getenv("SOLR_CORE") + "/select")
					.addHeader("Accept", "application/json")
					.execute()
					.returnContent()
					.asString();
			return true;
		} catch (IOException e)
		{
			return false;
		}
	}

	/**
	 * No close logic needed, since no continuous connection is built.
	 */
	@Override
	public void close()
	{

	}

	public String getEndpoint()
	{
		return this.rootEndpoint;
	}
}
