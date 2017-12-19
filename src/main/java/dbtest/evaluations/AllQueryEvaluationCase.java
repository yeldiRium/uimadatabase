package dbtest.evaluations;

import com.google.common.collect.Iterators;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.EvaluationCase;
import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiReader;
import org.apache.commons.io.FileUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.hucompute.services.uima.database.neo4j.data.Const;
import org.hucompute.services.uima.database.neo4j.impl.Document_Neo4J_Impl;
import org.hucompute.services.uima.database.neo4j.impl.Lemma_Neo4J_Impl;
import org.hucompute.services.uima.database.neo4j.impl.MDB_Neo4J_Impl;
import org.hucompute.services.uima.database.neo4j.impl.Pos_Neo4J_Impl;
import org.hucompute.services.uima.database.xmi.POSCounter;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

public class AllQueryEvaluationCase implements EvaluationCase
{
	@Override
	public ConnectionRequest requestConnection()
	{
		// TODO: decide what to request here
		return null;
	}

	@Override
	public void run(ConnectionResponse connectionResponse)
	{

		try
		{
			AllQueryEvaluationCase.getPosXMI();
			AllQueryEvaluationCase.getLemmaXMI();

			AllQueryEvaluationCase.getPosDistributionNeo4j();
			AllQueryEvaluationCase.getLemmaDistributionNeo4j();
			AllQueryEvaluationCase.getTypeTokenDistributionNeo4j();
		} catch (UIMAException | IOException e)
		{
			e.printStackTrace();
		}
	}

	public static void getPosXMI() throws UIMAException, IOException
	{
		long start = System.currentTimeMillis();
		CollectionReader xmireader = CollectionReaderFactory.createReader(
				XmiReaderModified.class,
				XmiReader.PARAM_SOURCE_LOCATION,
				System.getenv("INPUT_DIR"),
				XmiReader.PARAM_PATTERNS,
				"[+]**/*.xmi.gz",
				XmiReader.PARAM_LANGUAGE,
				"de"
		);

		runPipeline(
				xmireader,
				createEngine(POSCounter.class)
		);
		FileUtils.writeStringToFile(
				new File("output/AllQueryEvaluationCase_xmiSumPos.log"),
				"" + (System.currentTimeMillis() - start)
		);
		System.out.println(System.currentTimeMillis() - start);
	}

	public static void getLemmaXMI() throws UIMAException, IOException
	{
		long start = System.currentTimeMillis();
		CollectionReader xmireader = CollectionReaderFactory.createReader(
				XmiReaderModified.class,
				XmiReader.PARAM_SOURCE_LOCATION,
				System.getenv("INPUT_DIR"),
				XmiReader.PARAM_PATTERNS,
				"[+]**/*.xmi.gz",
				XmiReader.PARAM_LANGUAGE,
				"de"
		);

		runPipeline(
				xmireader,
				createEngine(POSCounter.class)
		);
		FileUtils.writeStringToFile(
				new File("output/AllQueryEvaluationCase_xmiSumLemma.log"),
				"" + (System.currentTimeMillis() - start)
		);
		System.out.println(System.currentTimeMillis() - start);
	}

	public static void getPosDistributionNeo4j() throws UIMAException, IOException
	{
		long start = System.currentTimeMillis();

		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl(
				"/home/ahemati/workspace/XMI4Neo4J/conf.conf"
		);

		Iterator<Node> pos =
				pMDB.getNodes(Pos_Neo4J_Impl.getLabel()).iterator();
		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
		{
			pos.forEachRemaining(n -> {
				System.out.println(n.getProperty("value"));
				System.out.println(Iterators.size(
						n.getRelationships(
								Direction.INCOMING,
								Const.RelationType.pos
						).iterator()));
			});
		}
		FileUtils.writeStringToFile(
				new File("output/AllQueryEvaluationCase_neo4jSumPos.log"),
				"" + (System.currentTimeMillis() - start)
		);
		System.out.println(System.currentTimeMillis() - start);
	}

	static int i = 0;

	public static void getLemmaDistributionNeo4j()
			throws UIMAException, IOException
	{
		long start = System.currentTimeMillis();
		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl(
				"/home/ahemati/workspace/XMI4Neo4J/conf.conf"
		);
		Iterator<Node> pos =
				pMDB.getNodes(Lemma_Neo4J_Impl.getLabel()).iterator();

		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
		{

			pos.forEachRemaining(n -> {
				if (i++ % 1000 == 0)
					System.out.println(i);
				Iterators.size(
						n.getRelationships(
								Direction.INCOMING,
								Const.RelationType.lemma
						).iterator());
			});
			System.out.println(i);
		}
		FileUtils.writeStringToFile(
				new File("output/AllQueryEvaluationCase_neo4jSumLemma.log"),
				"" + (System.currentTimeMillis() - start)
		);
		System.out.println(System.currentTimeMillis() - start);
	}

	public static void getLemmaDistributionBaseX()
			throws UIMAException, IOException
	{
		long start = System.currentTimeMillis();
		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl(
				"/home/ahemati/workspace/XMI4Neo4J/conf.conf"
		);
		Iterator<Node> pos =
				pMDB.getNodes(Lemma_Neo4J_Impl.getLabel()).iterator();

		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
		{

			pos.forEachRemaining(n -> {
				if (i++ % 1000 == 0)
					System.out.println(i);
				Iterators.size(
						n.getRelationships(
								Direction.INCOMING,
								Const.RelationType.lemma
						).iterator());
			});
			System.out.println(i);
		}
		FileUtils.writeStringToFile(
				new File("output/AllQueryEvaluationCase_neo4jSumLemma.log"),
				"" + (System.currentTimeMillis() - start)
		);
		System.out.println(System.currentTimeMillis() - start);
	}

	public static void getTypeTokenDistributionNeo4j()
			throws UIMAException, IOException
	{
		long start = System.currentTimeMillis();
		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl(
				"/home/ahemati/workspace/XMI4Neo4J/conf.conf"
		);
		Iterator<Node> documents =
				pMDB.getNodes(Document_Neo4J_Impl.getLabel()).iterator();

		try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx())
		{

			documents.forEachRemaining(n -> {

//				System.out.println(n);
//				if(i++%1000==0)
//					System.out.println(i);
//				Iterators.size(
//						n.getRelationships(
//								Direction.INCOMING,
//								Const.RelationType.lemma
//						).iterator());
			});
			System.out.println(i);
		}
		FileUtils.writeStringToFile(
				new File("output/AllQueryEvaluationCase_neo4jTypeToken.log"),
				"" + (System.currentTimeMillis() - start)
		);
		System.out.println(System.currentTimeMillis() - start);
	}
}
