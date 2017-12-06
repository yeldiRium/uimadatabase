package main;

import org.hucompute.services.uima.database.neo4j.impl.MDB_Neo4J_Impl;
import org.hucompute.services.uima.database.neo4j.impl.Pos_Neo4J_Impl;
import org.hucompute.services.uima.database.neo4j.impl.Token_Neo4J_Impl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class Main {

	public static void main(String[] args) {
		MDB_Neo4J_Impl pMDB = new MDB_Neo4J_Impl("/home/ahemati/workspace/XMI4Neo4J/conf.conf");

		Token_Neo4J_Impl token = Token_Neo4J_Impl.create(pMDB);

		Pos_Neo4J_Impl pos = Pos_Neo4J_Impl.getOrCreate(pMDB, "NN");
		token.setPos(pos);

		for (Node node : pMDB.getNodes(Token_Neo4J_Impl.getLabel())) {
			try (Transaction tx = MDB_Neo4J_Impl.gdbs.beginTx()) {
				tx.acquireReadLock(node);
				node.getRelationships().forEach(n->{
					System.out.println(n);
				});
			}
		}

	}

}
