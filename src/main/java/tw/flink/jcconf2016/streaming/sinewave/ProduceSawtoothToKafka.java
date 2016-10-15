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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import tw.flink.jcconf2016.streaming.sinewave.datamodel.UnKeyedDataPointSchema;
import tw.flink.jcconf2016.streaming.sinewave.datamodel.UnkeyedDataPoint;
import tw.flink.jcconf2016.streaming.sinewave.source.SawtoothSource;

public class ProduceSawtoothToKafka {

	private static final String KAFKA_BROKER_SERVER = "localhost:9092";
	private static final String SAWTOOTH_TOPIC = "sawtoothWave";

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// generate the sawtooth wave datapoints
		// a total of 40 steps, with 100 ms interval between each point = 4 sec period
		DataStream<UnkeyedDataPoint> originalSawTooth =
				env.addSource(new SawtoothSource(100, 40, 1));

		originalSawTooth.addSink(
				new FlinkKafkaProducer09<>(
						KAFKA_BROKER_SERVER,
						SAWTOOTH_TOPIC,
						new UnKeyedDataPointSchema()
				)
		);

		env.execute();
	}

}
