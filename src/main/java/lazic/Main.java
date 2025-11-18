package lazic;

import lazic.sources.NzGdp;
import lazic.utils.ingest.IngestManager;

public class Main {
	public static void main(String[] args) {
		NzGdp nzGdp = new NzGdp();
		var x = nzGdp.getDataPoints();
		IngestManager.INSTANCE.fetchDataFromSources();
		System.out.print(IngestManager.INSTANCE.data);
	}
}