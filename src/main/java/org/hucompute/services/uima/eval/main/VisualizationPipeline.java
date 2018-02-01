package org.hucompute.services.uima.eval.main;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.hucompute.services.uima.eval.utility.logging.PlainFormatter;
import org.hucompute.services.uima.eval.visualization.DataSet;
import org.hucompute.services.uima.eval.visualization.GraphToTexWriter;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Creates .tex graphs for all the acquired data.
 */
public class VisualizationPipeline
{
	protected static class OutputFileData
	{
		protected final String fileName;
		protected final String eval;
		protected final String dbName;
		protected final int fileCount;

		public OutputFileData(String fileName, String eval, String dbName, int fileCount)
		{
			this.fileName = fileName;
			this.eval = eval;
			this.dbName = dbName;
			this.fileCount = fileCount;
		}
	}

	protected static Logger logger = Logger.getLogger(VisualizationPipeline.class.getName());

	public static void main(String[] args) throws IOException
	{
		// Clean up logging to stdout
		Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(Level.ALL);
		Formatter plainFormatter = new PlainFormatter();
		for (Handler h : rootLogger.getHandlers())
		{
			h.setFormatter(plainFormatter);
			h.setLevel(Level.ALL);
		}

		logger.info("Starting Visualization pipeline.");

		Pattern outputFilePattern = Pattern.compile("(?<eval>.+)_(?<db>.+)_(?<fileCount>\\d+)\\.txt");

		List<String> fileList = Lists.newArrayList(
				new File(System.getenv("OUTPUT_DIR")).list()
		);
		fileList.forEach(blub -> logger.info(blub));

		// Iterate over all output files in the output directory.
		// For each of them, create an OutputFileData object with all needed
		// information for further processing.
		// Then for each file, load its contents into a JSONObject, then iterate
		// over all method results in there.
		// For each of those create (if needed) a Map from DBName to DataSet.
		// Then create (if needed) the according DataSet and add the current in-
		// putFileCount and time value.
		Map<String, Map<String, DataSet<Number>>> plottableResults = fileList
				.parallelStream()
				.map(file -> {
					Matcher match = outputFilePattern.matcher(file);
					if (match.matches())
					{
						return new OutputFileData(
								match.group(0),
								match.group(1),
								match.group(2),
								Integer.parseInt(match.group(3))
						);
					} else
					{
						return null;
					}
				})
				.filter(Objects::nonNull)
				.collect(
						ConcurrentHashMap::new,
						(Map<String, Map<String, DataSet<Number>>> map,
						 OutputFileData fileData) -> {
							try
							{
								JSONObject fileContent = new JSONObject(
										IOUtils.toString(new FileInputStream(
												new File(
														System.getenv("OUTPUT_DIR") + "/" + fileData.fileName
												)
										))
								);
								fileContent.keySet().forEach(testName -> {
									JSONObject testData = fileContent.optJSONObject(testName);
									if (testData == null)
										return;

									String plotName = fileData.eval + "_" +
											testName;

									Map<String, DataSet<Number>> plottableSets;
									if (!map.containsKey(plotName))
									{
										plottableSets =
												new ConcurrentHashMap<>();
										map.put(plotName, plottableSets);
									} else
									{
										plottableSets = map.get(plotName);
									}

									DataSet<Number> aDataSet;
									if (!plottableSets.containsKey(
											fileData.dbName
									))
									{
										aDataSet = new DataSet<>(
												fileData.dbName
										);
										plottableSets.put(
												fileData.dbName, aDataSet
										);
									} else
									{
										aDataSet = plottableSets.get(
												fileData.dbName
										);
									}
									Vector<Number> aVector = new Vector<>(2, 0);
									aVector.add(fileData.fileCount);
									logger.info("parsing data for " + plotName + " in db " + fileData.dbName);
									aVector.add(Double.parseDouble(testData.get("avgTime").toString()));
									aDataSet.addValue(aVector);
								});
							} catch (IOException e)
							{
								e.printStackTrace();
							}
						},
						(Map<String, Map<String, DataSet<Number>>> map1,
						 Map<String, Map<String, DataSet<Number>>> map2) -> {
							map2.forEach((testName, testData2) -> {
								if (!map1.containsKey(testName))
								{
									map1.put(testName, testData2);
								} else
								{
									Map<String, DataSet<Number>> testData1 =
											map1.get(testName);
									testData2.forEach((dbName, dataSet) -> {
										if (!testData1.containsKey(dbName))
										{
											testData1.put(dbName, dataSet);
										} else
										{
											testData1.get(dbName)
													.addValue(dataSet);
										}
									});
								}
							});
						}
				);

		for (Map.Entry<String, Map<String, DataSet<Number>>> entry :
				plottableResults.entrySet())
		{
			File graphDir =
					new File(System.getenv("OUTPUT_DIR") + "/graphs");
			if (!graphDir.exists())
			{
				graphDir.mkdir();
			}

			File graphFile = new File(
					System.getenv("OUTPUT_DIR") + "/graphs/" +
							entry.getKey() + ".tex"
			);

			if (graphFile.exists())
			{
				graphFile.delete();
			}
			graphFile.createNewFile();

			IOUtils.write(
					GraphToTexWriter.linearGraph(
							Lists.newArrayList(entry.getValue().values()),
							entry.getKey(),
							"Time [ms]",
							"Documents"
					),
					new FileOutputStream(graphFile));
		}

		logger.info("Ending Visualization pipeline.");
	}
}
