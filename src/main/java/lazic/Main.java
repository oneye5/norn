package lazic;

import lazic.sources.*;
import lazic.utils.ingest.IngestManager;

public class Main {
	public static void main(String[] args) {
		//NzGdp nzGdp = new NzGdp();
		NzRatesFx yfPrices = new NzRatesFx();
		var x = yfPrices.getDataPoints();

		IngestManager.INSTANCE.fetchDataFromSources();
		IngestManager.INSTANCE.printSubset(100);
	}
}