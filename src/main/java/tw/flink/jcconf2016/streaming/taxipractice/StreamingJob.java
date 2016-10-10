/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tw.flink.jcconf2016.streaming.taxipractice;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import tw.flink.jcconf2016.streaming.taxipractice.datamodel.TaxiRide;
import tw.flink.jcconf2016.streaming.taxipractice.source.TaxiRideSource;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/jcconf2016-workshop-0.1.jar
 * From the CLI you can then run
 * 		./bin/flink run -c tw.flink.jcconf2016.streaming.taxipractice.StreamingJob target/jcconf2016-workshop-0.1.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.addSource(new TaxiRideSource("/Users/tzulitai/Repos/github/jcconf2016-workshop/nycTaxiRides.gz", 0, 600))
				.addSink(new SocketClientSink<>("localhost", 1111, new SerializationSchema<TaxiRide>() {
					@Override
					public byte[] serialize(TaxiRide taxiRide) {
						return taxiRide.toString().getBytes();
					}
				}));

		env.execute("Flink Streaming Java API Skeleton");
	}
}
