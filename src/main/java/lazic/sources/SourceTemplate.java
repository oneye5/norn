package lazic.sources;

import com.google.gson.Gson;
import lazic.sources.config.Tickers;
import lazic.utils.ingest.DataPoint;
import lazic.utils.ingest.DataSourceBase;
import lazic.utils.ingest.WebHtmlGetter;

import java.time.LocalDateTime;
import java.util.Set;

public class SourceTemplate extends DataSourceBase {
	private final String URL = "";

	/**
	 * Returns a set of DataPoint's. Ticker is null if the datapoint does not pertain to a particular ticker, such as macroeconomic data for example
	 * There are multiple DataPoint's in a time-series feature, and there may be multiple features returned overall.
	 */
	@Override
	public Set<DataPoint> getDataPoints() {

		DataPoint example = new DataPoint(LocalDateTime.now(), "Ticker", "Feature name", Double.valueOf(1));

		String[] tickers = lazic.sources.config.Tickers.TICKERS; //"ANZ.NZ", "AFCA.NZ", etc
		Gson gson = new Gson();
		String rawData = WebHtmlGetter.get(URL);
		System.out.println(rawData);
		return Set.of(example);
	}
}
