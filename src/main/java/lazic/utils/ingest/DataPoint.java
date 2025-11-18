package lazic.utils.ingest;
import java.io.Serializable;
import java.time.LocalDateTime;

public class DataPoint implements Serializable {

	private LocalDateTime timestamp;
	private String ticker;          // optional, may be null for macro data
	private String featureName;     // e.g. "close_price", "inflation", "EPS"
	private double value;           // exactly one feature
	private String source;          // where it came from

	public DataPoint() {}

	public DataPoint(LocalDateTime timestamp,
									 String ticker,
									 String featureName,
									 double value,
									 String source) {
		this.timestamp = timestamp;
		this.ticker = ticker;
		this.featureName = featureName;
		this.value = value;
		this.source = source;
	}

	public LocalDateTime getTimestamp() { return timestamp; }
	public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

	public String getTicker() { return ticker; }
	public void setTicker(String ticker) { this.ticker = ticker; }

	public String getFeatureName() { return featureName; }
	public void setFeatureName(String featureName) { this.featureName = featureName; }

	public double getValue() { return value; }
	public void setValue(double value) { this.value = value; }

	public String getSource() { return source; }
	public void setSource(String source) { this.source = source; }
}
