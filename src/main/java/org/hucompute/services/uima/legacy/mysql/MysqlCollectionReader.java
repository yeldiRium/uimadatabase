package org.hucompute.services.uima.legacy.mysql;

import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.hucompute.services.uima.legacy.AbstractCollectionReader;

import java.io.IOException;


public class MysqlCollectionReader extends AbstractCollectionReader
{

	@Override
	public void initialize(UimaContext context)
			throws ResourceInitializationException
	{
		super.initialize(context);
		//initialize jdbc connection
		//get curser of data.
	}

	//bearbeiten
	public boolean hasNext() throws IOException, CollectionException
	{
		return false;
	}

	@Override
	public void getNext(CAS aCAS) throws IOException, CollectionException
	{
		resumeWatch();
		//readCAS()
		suspendWatch();
		log();
	}

	@Override
	public Progress[] getProgress()
	{
		// TODO Auto-generated method stub
		return null;
	}
}
