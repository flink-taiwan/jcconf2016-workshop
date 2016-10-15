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
package tw.flink.jcconf2016.streaming.sinewave;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import tw.flink.jcconf2016.streaming.sinewave.datamodel.KeyedDataPoint;
import tw.flink.jcconf2016.streaming.sinewave.datamodel.UnKeyedDataPointSchema;
import tw.flink.jcconf2016.streaming.sinewave.datamodel.UnkeyedDataPoint;
import tw.flink.jcconf2016.streaming.sinewave.sink.InfluxDBSink;

import javax.annotation.Nullable;
import java.util.Properties;

public class SinewaveSumFromKafka {

	private static final String KAFKA_BROKER_SERVER = "localhost:9092";
	private static final String SAWTOOTH_TOPIC = "sawtoothWave";

	private static final String SINEWAVE_SUM_GROUP = "sinewaveSumGroup";

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// generate the sawtooth wave datapoints
		// a total of 40 steps, with 100 ms interval between each point = 4 sec period

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", KAFKA_BROKER_SERVER);
		props.setProperty("group.id", SINEWAVE_SUM_GROUP);
		props.setProperty("auto.commit.enable", "false");

		DataStream<UnkeyedDataPoint> originalSawTooth = env.addSource(
				new FlinkKafkaConsumer09<>(SAWTOOTH_TOPIC, new UnKeyedDataPointSchema(), props));

		originalSawTooth.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<UnkeyedDataPoint>() {
			@Nullable
			@Override
			public Watermark checkAndGetNextWatermark(UnkeyedDataPoint unkeyedDataPoint, long l) {
				return new Watermark(l);
			}

			@Override
			public long extractTimestamp(UnkeyedDataPoint unkeyedDataPoint, long l) {
				return unkeyedDataPoint.timestampMillis;
			}
		});

		// attach key to the generated sawtooth
		DataStream<KeyedDataPoint> sawtoothKeyed = originalSawTooth
				.map(new AttachKeyToDataPoint("sawtooth"));

		// map the generated sawtooth to a sine wave,
		// and also attach key to the sine wave
		DataStream<KeyedDataPoint> sinewaveKeyed = originalSawTooth
				.map(new SawtoothToSinewave())
				.map(new AttachKeyToDataPoint("sinewave"));

		// we want to send both sawtooth and sine to InfluxDB, so union
		DataStream<KeyedDataPoint> completeKeyedStream =
				sawtoothKeyed.union(sinewaveKeyed);
		completeKeyedStream.addSink(new InfluxDBSink("sensors"));

		// windowing to sum up the datapoint values of the waves (key by "sawtooth" and "sinewave")
		completeKeyedStream
				.keyBy("key")
				.timeWindow(Time.seconds(4)) // 40 data points, 100 ms interval = 4 seconds
				.sum("value")
				.addSink(new InfluxDBSink("sensors-summed"));

		env.execute();
	}

	public static class AttachKeyToDataPoint implements MapFunction<UnkeyedDataPoint, KeyedDataPoint> {
		private String key;

		public AttachKeyToDataPoint(String key) {
			this.key = key;
		}

		@Override
		public KeyedDataPoint map(UnkeyedDataPoint unkeyedDataPoint) throws Exception {
			return new KeyedDataPoint(key, unkeyedDataPoint.timestampMillis, unkeyedDataPoint.value);
		}
	}

	public static class SawtoothToSinewave implements MapFunction<UnkeyedDataPoint, UnkeyedDataPoint> {
		@Override
		public UnkeyedDataPoint map(UnkeyedDataPoint sawtooth) throws Exception {
			return new UnkeyedDataPoint(
					sawtooth.timestampMillis,
					Math.sin(sawtooth.value * 2 * Math.PI));
		}
	}

}
