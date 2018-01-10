package dbtest.queryHandler.implementation.ArangoDB.model;

import com.arangodb.entity.DocumentField;

/**
 * Edges currently only hold their relationships.
 *
 * This class can be expanded/subclassed, if more values are needed.
 */
public class Edge
{
	@DocumentField(DocumentField.Type.FROM)
	protected String from;

	@DocumentField(DocumentField.Type.TO)
	protected String to;

}
