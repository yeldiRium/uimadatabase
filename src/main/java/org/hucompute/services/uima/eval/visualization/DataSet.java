package org.hucompute.services.uima.eval.visualization;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

public class DataSet<T extends Number & Comparable> {
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
		this.values.sort(Comparator.comparing(v -> v.get(0)));
	}

	public void addValue(DataSet<T> aDataSet)
	{
		this.values.addAll(aDataSet.getValues());
	}
}
