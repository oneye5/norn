package lazic;

import lazic.sources.NzGdp;
import lazic.sources.YfPrices;
import lazic.utils.ingest.IngestManager;

public class Main {
	public static void main(String[] args) {
		//NzGdp nzGdp = new NzGdp();
		YfPrices yfPrices = new YfPrices();
		var x = yfPrices.getDataPoints();

		IngestManager.INSTANCE.fetchDataFromSources();
		IngestManager.INSTANCE.printSubset(100);
	}
}