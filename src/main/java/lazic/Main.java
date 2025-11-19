package lazic;

import lazic.sources.*;
import lazic.utils.ingest.CsvLongParser;
import lazic.utils.ingest.IngestManager;

import java.nio.file.Path;

public class Main {
	public static void main(String[] args) {
		new NzBusinessConfidence();
		new NzGdp();
		new NzRatesFx();
		new NzVehicleRegistrations();
		new YfFinances();
		new YfPrices();

		IngestManager.INSTANCE.fetchDataFromSources();
		IngestManager.INSTANCE.printSubset(100);
		CsvLongParser.saveCsv(Path.of("all_data_long.csv").toAbsolutePath().toString());
	}
}