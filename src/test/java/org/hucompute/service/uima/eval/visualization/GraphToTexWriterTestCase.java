package org.hucompute.service.uima.eval.visualization;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.hucompute.services.uima.eval.visualization.DataSet;
import org.hucompute.services.uima.eval.visualization.GraphToTexWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphToTexWriterTestCase
{
	@Test
	public void Given_GraphToTexWriterAndTestDataSet_When_WritingLinearGraph_Then_CorrectTexStringIsCreated() throws IOException
	{
		DataSet<Number> one = new DataSet<>("one");
		one.addValue(new Vector<>(Lists.newArrayList(1, 1)));
		one.addValue(new Vector<>(Lists.newArrayList(2, 2)));
		one.addValue(new Vector<>(Lists.newArrayList(3, 3)));
		one.addValue(new Vector<>(Lists.newArrayList(4, 4)));
		one.addValue(new Vector<>(Lists.newArrayList(5, 5)));
		one.addValue(new Vector<>(Lists.newArrayList(6, 6)));

		DataSet<Number> two = new DataSet<>("two");
		two.addValue(new Vector<>(Lists.newArrayList(1, 5)));
		two.addValue(new Vector<>(Lists.newArrayList(2, 1)));
		two.addValue(new Vector<>(Lists.newArrayList(3, 3)));
		two.addValue(new Vector<>(Lists.newArrayList(4, 2)));
		two.addValue(new Vector<>(Lists.newArrayList(5, 2)));
		two.addValue(new Vector<>(Lists.newArrayList(6, 2)));

		List<DataSet<Number>> dataList = new ArrayList<>();
		dataList.add(one);
		dataList.add(two);

		GraphToTexWriter writer = new GraphToTexWriter();
		String texGraph = writer.linearGraph(
				dataList,
				"testTitle",
				"testXLabel",
				"testYLabel"
		);

		String expectedGraph = IOUtils.toString(new FileInputStream(new File(
				"src/test/resources/visualization/linearGraphResult.tex"
		)));

		assertEquals(expectedGraph, texGraph);
	}

	@Test
	public void Given_GraphToTexWriterAndTestDataSet_When_WritingLogarithmicGraph_Then_CorrectTexStringIsCreated() throws IOException
	{
		DataSet<Number> one = new DataSet<>("one");
		one.addValue(new Vector<>(Lists.newArrayList(1, 1)));
		one.addValue(new Vector<>(Lists.newArrayList(2, 2)));
		one.addValue(new Vector<>(Lists.newArrayList(3, 3)));
		one.addValue(new Vector<>(Lists.newArrayList(4, 4)));
		one.addValue(new Vector<>(Lists.newArrayList(5, 5)));
		one.addValue(new Vector<>(Lists.newArrayList(6, 6)));

		DataSet<Number> two = new DataSet<>("two");
		two.addValue(new Vector<>(Lists.newArrayList(1, 5)));
		two.addValue(new Vector<>(Lists.newArrayList(2, 1)));
		two.addValue(new Vector<>(Lists.newArrayList(3, 3)));
		two.addValue(new Vector<>(Lists.newArrayList(4, 2)));
		two.addValue(new Vector<>(Lists.newArrayList(5, 2)));
		two.addValue(new Vector<>(Lists.newArrayList(6, 2)));

		List<DataSet<Number>> dataList = new ArrayList<>();
		dataList.add(one);
		dataList.add(two);

		GraphToTexWriter writer = new GraphToTexWriter();
		String texGraph = writer.logarithmicGraph(
				dataList,
				"testTitle",
				"testXLabel",
				"testYLabel"
		);

		String expectedGraph = IOUtils.toString(new FileInputStream(new File(
				"src/test/resources/visualization/logarithmicGraphResult.tex"
		)));

		assertEquals(expectedGraph, texGraph);
	}
}
