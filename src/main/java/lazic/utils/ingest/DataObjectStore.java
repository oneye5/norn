package lazic.utils.ingest;

import java.util.HashSet;
import java.util.Set;

// singleton
public enum DataObjectStore {
	INSTANCE;
	public final Set<DataSourceBase> sources = new HashSet<>();
	public final Set<DataPoint> data = new HashSet<DataPoint>();
}
