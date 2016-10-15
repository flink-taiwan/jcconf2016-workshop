/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tw.flink.jcconf2016.streaming.sinewave.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import tw.flink.jcconf2016.streaming.sinewave.datamodel.KeyedDataPoint;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink extends RichSinkFunction<KeyedDataPoint> {

	private transient InfluxDB influxDB = null;
	private static String dataBaseName = "JCConf2016";
	private static String fieldName = "value";
	private String measurement;

	public InfluxDBSink(String measurement){
		this.measurement = measurement;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		influxDB = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin");
		influxDB.createDatabase(dataBaseName);
		influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public void invoke(KeyedDataPoint dataPoint) throws Exception {
		Point.Builder builder = Point.measurement(measurement)
				.time(dataPoint.timestampMillis, TimeUnit.MILLISECONDS)
				.addField(fieldName, dataPoint.value)
				.tag("key", dataPoint.key);

		influxDB.write(dataBaseName, "default", builder.build());
	}
}
