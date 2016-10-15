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
package tw.flink.jcconf2016.streaming.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Basic streaming word count example, that reads text lines from a socket stream
 */
public class StreamingWordCount {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String,Integer>> counts = env
				// read stream of words from socket
				.socketTextStream("localhost", 1234)
				// split up the lines into tuple: (word, 1)
				.flatMap(new LineSplitter())
				// use the “word” as key
				.keyBy(0)
				// compute counts every 10 seconds
				.timeWindow(Time.seconds(10))
				// sum up the values
				.sum(1);

		// print result to console
		counts.print();
		// execute program
		env.execute("Socket Word Count Example");
	}

	public static class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
			// normalize and split lines
			String[] words = value.toLowerCase().split("\\W+");
			for (String word : words) {
				if (word.length() > 0) {
					out.collect(new Tuple2<>(word, 1));
				}
			}
		}

	}

}
