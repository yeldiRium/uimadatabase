package org.hucompute.services.uima.eval.database.connection.implementation;

import org.hucompute.services.uima.eval.database.connection.Connection;
import org.jsoup.Jsoup;

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
			Jsoup.connect(this.rootEndpoint + "/bigdata/sparql").get();
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
