package org.hucompute.services.uima.eval.database.connection.implementation;

import org.apache.http.client.fluent.Request;
import org.hucompute.services.uima.eval.database.connection.Connection;

import java.io.IOException;

public class BlazegraphConnection extends Connection
{
	protected String rootEndpoint;

	public BlazegraphConnection()
	{
		this.rootEndpoint = "http://" + System.getenv("BLAZEGRAPH_HOST") +
				":" + System.getenv("BLAZEGRAPH_PORT");
	}

	@Override
	protected boolean tryToConnect()
	{
		try
		{
			// Just to test, if the server responds.
			Request.Get(this.rootEndpoint + "/bigdata/sparql")
					.addHeader("Accept", "application/sparql-results+json")
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
