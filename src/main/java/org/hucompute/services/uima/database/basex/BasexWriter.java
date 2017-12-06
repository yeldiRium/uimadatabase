package org.hucompute.services.uima.database.basex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.AbstractWriter;
import org.xml.sax.SAXException;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;

public class BasexWriter extends AbstractWriter {
		
	final String dbName = "testdb2_small";
	final String host = "localhost";
	final int port = 1984;
	final String user = "admin";
	final String pass = "admin";

	BaseXClient session = null;
	
	@Override
	public void initialize(UimaContext context)
			throws ResourceInitializationException {
		super.initialize(context);
		try {
			session = new BaseXClient(host, port, user, pass);
			
			// Create DB if not exists
			session.execute("CHECK " + dbName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException {
		resumeWatch();
		String path = "cas/" + DocumentMetaData.get(jCas).getDocumentId();
		ByteArrayOutputStream docOS = new ByteArrayOutputStream();
		try {
			XmiCasSerializer.serialize(jCas.getCas(), docOS);
		} catch (SAXException e1) {
			e1.printStackTrace();
		}
		try {
			session.replace(path, new ByteArrayInputStream(docOS.toByteArray()));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	      
		suspendWatch();
		log();
	}
}