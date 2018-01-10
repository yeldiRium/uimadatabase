package dbtest.queryHandler.implementation.ArangoDB.model;

public class Token
{
	protected String documentId;
	protected int begin;
	protected int end;
	protected String value;

	public String getDocumentId()
	{
		return documentId;
	}

	public void setDocumentId(String documentId)
	{
		this.documentId = documentId;
	}

	public int getBegin()
	{
		return begin;
	}

	public void setBegin(int begin)
	{
		this.begin = begin;
	}

	public int getEnd()
	{
		return end;
	}

	public void setEnd(int end)
	{
		this.end = end;
	}

	public String getValue()
	{
		return value;
	}

	public void setValue(String value)
	{
		this.value = value;
	}
}
