package org.hucompute.services.uima.eval.legacy.mongo;
//package org.apache.uima.mongo;
//
//import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
//import static org.apache.uima.fit.factory.CollectionReaderFactory.createReader;
//import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;
//import static org.apache.uima.fit.util.JCasUtil.select;
//import static org.apache.uima.mongo.MongoCollectionReader.PARAM_DB_CONNECTION;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//import java.util.Collection;
//import java.util.Iterator;
//
//import org.apache.uima.UIMAException;
//import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
//import org.apache.uima.fit.factory.JCasFactory;
//import org.apache.uima.jcas.JCas;
//import org.apache.uima.resource.ResourceInitializationException;
//import org.apache.uima.test.Token;
//
//import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;
//import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordPosTagger;
//import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordSegmenter;
//
//public class EvaluationPipeline {
//	public static void main(String...args) throws UIMAException{
//        JCas jCas = JCasFactory.createJCas();
//        jCas.setDocumentText("this is just a test.");
//        jCas.setDocumentLanguage("en");
//        
//        runPipeline(jCas,
//        		createEngine(StanfordSegmenter.class),
//        		createEngine(StanfordPosTagger.class),
//        		createEngine(StanfordLemmatizer.class),
//                createEngine(MongoWriter.class, PARAM_DB_CONNECTION, new String[]{"pc-108-201","test","wikipedia","",""}));
//
////        Iterator<JCas> iterator = iterator(createReader(
////                MongoCollectionReader.class, PARAM_DB_CONNECTION, conn));
////        JCas next = iterator.next();
////        assertEquals("this is just a test", next.getDocumentText());
////        Collection<Token> tokens = select(jCas, Token.class);
////        assertEquals(1, tokens.size());
//	}
//
//}
