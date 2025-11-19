package lazic.utils.ingest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

// singleton
public enum IngestManager {
	INSTANCE;
	public final Set<DataSourceBase> sources = new HashSet<>();
	public final Set<DataPoint> data = new HashSet<DataPoint>();

	public void fetchDataFromSources() {
		data.clear();
		sources.parallelStream().forEach(source -> {
			var dataPoints = source.getDataPoints();
			dataPoints = dataPoints.stream()
							.filter(dp->dp.getValue() != null)
							.collect(Collectors.toSet());

			this.data.addAll(dataPoints);
		});
	}

	public void printSubset(int count) {
		var asList = new java.util.ArrayList<>(data);
		Collections.shuffle(asList);

		asList.subList(0, Math.min(data.size(), count))
						.forEach(dp-> System.out.println(dp.toString()));
	}

	/**
	 * datapoint contents:
	 *
	 * public DataPoint(LocalDateTime timestamp,
	 * 									 String ticker,
	 * 									 String featureName,
	 * 									 Double value)
	 */
}
