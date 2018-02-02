package org.hucompute.services.uima.eval.visualization;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

/**
 * Supports up to 19 datasets in one graph. More different colors are not avai-
 * lable without further modification.
 */
public class GraphToTexWriter
{
	protected static Logger logger = Logger.getLogger(
			GraphToTexWriter.class.getName()
	);

	protected static String[] colors = new String[]{
			"red", "green", "blue", "cyan", "magenta", "yellow", "black",
			"brown", "darkgray", "gray", "lightgray", "lime", "olive", "orange",
			"pink", "purple", "teal", "violet", "white"
	};

	protected static String escape(String text)
	{
		return text.replace("_", "\\_")
				.replace("[", "{[")
				.replace("]", "]}");
	}

	/**
	 * Creates a plot element for use in a pgfplot from a template and adds
	 * coordinate pairs and a color.
	 * @param dataSet A dataset which should be plotted.
	 * @param plotColor The color in which the set will be plotted.
	 * @return A String for use in .tex with the pgfplots library.
	 */
	protected static <T extends Number & Comparable> String makePlot(DataSet<T> dataSet, String plotColor)
			throws IOException
	{
		String template = IOUtils.toString(new FileInputStream(new File(
				"src/main/resources/templates/tex/plot.tex"
		)));
		StringBuilder plotLineBuilder = new StringBuilder();

		for (Vector<T> v : dataSet.getValues())
		{
			plotLineBuilder.append(String.format(
					"(%1$s,%2$s)%n", v.get(0), v.get(1))
			);
		}

		return template.replace(
				"%%PLOTCOORDINATES%%", plotLineBuilder.toString()
		).replace(
				"%%PLOTCOLOR%%", plotColor
		);
	}

	/**
	 * Fills a given template with all relevant graph data. Iterates over color
	 * array for multiple DataSets.
	 * @param template
	 * @param data
	 * @param title
	 * @param xLabel
	 * @param yLabel
	 * @return
	 * @throws IOException
	 */
	protected static <T extends Number & Comparable> String fillGraphTemplate(
			String template,
			List<DataSet<T>> data,
			String title,
			String xLabel,
			String yLabel
	) throws IOException
	{
		StringBuilder plots = new StringBuilder();
		StringBuilder legend = new StringBuilder();

		int colorIndex = 0;

		for (DataSet<T> aDataSet : data)
		{
			plots.append(makePlot(aDataSet, colors[colorIndex++]));
			legend.append((legend.length() > 0) ? "," : "")
					.append(aDataSet.getName());
		}

		return template
				.replace("%%TITLE%%", escape(title))
				.replace("%%XLABEL%%", escape(xLabel))
				.replace("%%YLABEL%%", escape(yLabel))
				.replace("%%LEGENDENTRIES%%", escape(legend.toString()))
				.replace("%%PLOTS%%", plots.toString());
	}

	/**
	 * Creates a .tex representation for a graph with logarithmic scales for
	 * multiple Datasets.
	 * @param data
	 * @return
	 */
	public static <T extends Number & Comparable> String logarithmicGraph(
			List<DataSet<T>> data,
			String title,
			String xLabel,
			String yLabel
	) throws IOException
	{
		String template = IOUtils.toString(new FileInputStream(new File(
				"src/main/resources/templates/tex/logarithmicGraph.tex"
		)));

		return fillGraphTemplate(template, data, title, xLabel, yLabel);
	}


	/**
	 * Creates a .tex representation for a graph with linear scales for multiple
	 * Datasets.
	 * @param data
	 * @return
	 */
	public static <T extends Number & Comparable> String linearGraph(
			List<DataSet<T>> data,
			String title,
			String xLabel,
			String yLabel
	) throws IOException
	{
		String template = IOUtils.toString(new FileInputStream(new File(
				"src/main/resources/templates/tex/linearGraph.tex"
		)));

		return fillGraphTemplate(template, data, title, xLabel, yLabel);
	}
}
