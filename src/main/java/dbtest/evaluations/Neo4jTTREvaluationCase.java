package dbtest.evaluations;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.connection.implementation.Neo4jConnection;
import dbtest.evaluationFramework.EvaluationCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.StopWatch;
import org.hucompute.services.uima.database.neo4j.Neo4jQueryHandler;
import org.hucompute.services.uima.database.neo4j.data.Const;

import java.io.File;
import java.io.IOException;

public class Neo4jTTREvaluationCase implements EvaluationCase
{
	@Override
	public ConnectionRequest requestConnection()
	{
		ConnectionRequest connectionRequest = new ConnectionRequest();
		connectionRequest.addRequestedConnection(Neo4jConnection.class);
		return connectionRequest;
	}

	@Override
	public void run(ConnectionResponse connectionResponse)
	{
		// TODO: create QueryHandler correctly after rewriting it
		Neo4jQueryHandler queryHandler = new Neo4jQueryHandler(new String[]{});
		StringBuilder builder = new StringBuilder();

		builder.append("Testing getTTR(List of 2):");
		long timeBegin = System.currentTimeMillis();
		queryHandler.getTTR(new String[]{"105","159"});
		builder.append("getTTR(List of 2) took: ")
				.append(System.currentTimeMillis() - timeBegin)
				.append("  ms\n")
				.append("Testing TTR_all():");
		timeBegin = System.currentTimeMillis();
		queryHandler.TTR_all();
		builder.append("TTR_all() took: ")
				.append(System.currentTimeMillis() - timeBegin)
				.append("  ms\n")
				.append("Testing count(LEMMA):");
		timeBegin = System.currentTimeMillis();
		queryHandler.count_type(Const.TYPE.LEMMA);
		builder.append("count(LEMMA) took: ")
				.append(System.currentTimeMillis() - timeBegin)
				.append("  ms\n")
				.append("Testing count(DocId, TOKEN):");
		timeBegin = System.currentTimeMillis();
		queryHandler.count_type_in_document("105",Const.TYPE.TOKEN);
		builder.append("count(DocId, TOKEN) took: ")
				.append(System.currentTimeMillis() - timeBegin)
				.append("  ms\n")
				.append("Testing count(LEMMA, Mensch):");
		timeBegin = System.currentTimeMillis();
		queryHandler.count_type_with_value(Const.TYPE.LEMMA, "Mensch");
		builder.append("count(LEMMA, Mensch) took: ")
				.append(System.currentTimeMillis() - timeBegin)
				.append(" ms\n");

		try
		{
			FileUtils.writeStringToFile(
				new File("output/Neo4jTTREvaluationCase.txt"),
				builder.toString()
			);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	// TODO: check if this is needed in the AllQueryEvaluationCase
	@Deprecated
	public static String testQueryTimes (Neo4jQueryHandler queryHandler, int runs) throws Exception {
		StopWatch a = new StopWatch();
		a.start();
		a.suspend();
		StopWatch b = new StopWatch();
		b.start();
		b.suspend();
		for(int i=0;i<runs;i++) {
			a.resume();
			queryHandler.get_Lemmata("1063");
			a.suspend();

			b.resume();
			queryHandler.getLemmata("1420");
			b.suspend();
		}
		return a.getTime() + "|" + b.getTime();
	}
}
