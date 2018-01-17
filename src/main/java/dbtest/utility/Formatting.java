package dbtest.utility;

import dbtest.queryHandler.implementation.BenchmarkQueryHandler;
import org.json.JSONObject;

import java.util.LongSummaryStatistics;

public class Formatting
{
	/**
	 * Creates generic statistics output for a given method name.
	 * Calculates count, min, avg, max and sum and puts them into a JSONObject.
	 *
	 * @param methodName   The method for which to create a statistic.
	 * @param queryHandler The BenchmarkQueryHandler from which to pull data.
	 * @return A JSONObject containing the stats in a predefined format.
	 */
	public static JSONObject createOutputForMethod(
			String methodName,
			BenchmarkQueryHandler queryHandler
	)
	{
		LongSummaryStatistics stats = queryHandler.getMethodBenchmarks()
				.get(methodName).getCallTimes().parallelStream()
				.mapToLong(Long::longValue).summaryStatistics();
		JSONObject statsJSONObject = new JSONObject();

		statsJSONObject.put("method", methodName);
		statsJSONObject.put("callCount", stats.getCount());
		statsJSONObject.put("minTime", stats.getMin());
		statsJSONObject.put("avgTime", stats.getAverage());
		statsJSONObject.put("maxTime", stats.getMax());
		statsJSONObject.put("sumTime", stats.getSum());
		statsJSONObject.put("more", new JSONObject());
		return statsJSONObject;
	}
}
