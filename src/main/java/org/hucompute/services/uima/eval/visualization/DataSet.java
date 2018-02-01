package org.hucompute.services.uima.eval.visualization;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class DataSet<T extends Number> {
	String name;
	List<Vector<T>> values;

	public DataSet(String name)
	{
		this.name = name;
		this.values = new ArrayList<>();
	}

	public String getName()
	{
		return name;
	}

	public List<Vector<T>> getValues()
	{
		return values;
	}

	public void addValue(Vector<T> value)
	{
		this.values.add(value);
	}

	public void addValue(DataSet<T> aDataSet)
	{
		this.values.addAll(aDataSet.getValues());
	}
}
