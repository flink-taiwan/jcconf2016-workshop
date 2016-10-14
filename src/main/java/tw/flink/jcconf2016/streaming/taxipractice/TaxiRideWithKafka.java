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
package tw.flink.jcconf2016.streaming.taxipractice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;
import tw.flink.jcconf2016.streaming.taxipractice.answer.TaxiRidePopularPlacesAnswer;
import tw.flink.jcconf2016.streaming.taxipractice.datamodel.TaxiRide;
import tw.flink.jcconf2016.streaming.taxipractice.datamodel.TaxiRideSchema;
import tw.flink.jcconf2016.streaming.taxipractice.source.TaxiRideSource;
import tw.flink.jcconf2016.streaming.taxipractice.util.GeoUtils;

import java.util.Properties;

/**
 * ------------------------------------------------------------------------
 * 		            EXERCISE #3: Taxi Ride With Kafka
 * ------------------------------------------------------------------------
 *
 * INSTRUCTIONS:
 *		Write the outputs of the first exercise (ride cleansing) to the Kafka
 *		topic "cleansedRides", and then read the events from the topic again
 *		to go through the second exercise (popular places). Note that you
 *		will need to assign watermarks to the source in order to use
 *		event time!
 *
 * HINTS:
 *		Use {@link FlinkKafkaConsumer09} to read from / write to Kafka topics.
 *		When you are initializing a {@link FlinkKafkaConsumer09}, you need to assign
 *		a serialization schema for the constructor. You can use {@link TaxiRideSchema}
 *		for this.
 *
 *		Because the source events will only be out-of-order by
 *		{@link TaxiRideWithKafka#MAX_EVENT_DELAY}, you can simply use
 *		{@link BoundedOutOfOrdernessTimestampExtractor} when assigning timestamps and
 *		watermarks for the Kafka source.
 *
 * PARAMETERS:
 * 		nycTaxiRidesPath - absolute path to the nycTaxiRides.gz file
 *
 * REFERENCE ANSWER:
 * 		{@link TaxiRidePopularPlacesAnswer}
 */
public class TaxiRideWithKafka {

	private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
	private static final String CLEANSED_RIDES_TOPIC = "cleansedRides";
	private static final String KAFKA_GROUP_ID = "popularPlacesGroup";

	private static final int MAX_EVENT_DELAY = 60;			// events are out of order by max 60 seconds
	private static final int SERVING_SPEED_FACTOR = 600;	// events of 10 minute are served in 1 second

	private static final int POPULAR_THRESHOLD = 20; // threshold for popular places

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String nycTaxiRidesPath = params.getRequired("nycTaxiRidesPath");

		// -------------------------------------------------------------------------------
		//		Clean the ride events and write them to Kafka (topic: CLEANSED_RIDES_TOPIC)
		// -------------------------------------------------------------------------------

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// ===============================================================================
		//		1. remember to set the auto watermark interval in the environment config
		// ===============================================================================

		// start the data generator
		DataStream<TaxiRide> rawRidesFromFile = env.addSource(
				new TaxiRideSource(nycTaxiRidesPath, MAX_EVENT_DELAY, SERVING_SPEED_FACTOR));

		DataStream<TaxiRide> filteredRides = rawRidesFromFile
				// filter out rides that do not start or stop in NYC
				.filter(new NYCFilter());

		// ===============================================================================
		//		2. write the cleansed events to the Kafka topic CLEANSED_RIDES_TOPIC;
		//		   the Kafka server location is at LOCAL_KAFKA_BROKER
		// ===============================================================================

		// -------------------------------------------------------------------------------
		//		Consume the cleansed ride events from Kafka and calculate popular places
		// -------------------------------------------------------------------------------

		// configure the Kafka consumer
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
		kafkaProps.setProperty("group.id", KAFKA_GROUP_ID);
		// always read the Kafka topic from the start
		kafkaProps.setProperty("auto.offset.reset", "earliest");

		// ===============================================================================
		//		3. replace "env.fromElements(new TaxiRide())" with a FlinkKafkaConsumer09
		//         that reads from the topic CLEANSED_RIDES_TOPIC.
		//		4. remember to assign watermarks to the events read from Kafka by calling
		//		   "assignTimestampsAndWatermarks". The events will at most be out-of-order
		//		   by MAX_EVENT_DELAY, so you can simply use a default
		//		   BoundedOutOfOrdernessTimestampExtractor for this.
		// ===============================================================================

		// create a TaxiRide data stream
		DataStream<TaxiRide> ridesFromKafka = env.fromElements(new TaxiRide());

		// find popular places
		DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularPlaces = ridesFromKafka
				// match ride to grid cell and event type (start or end)
				.map(new GridCellMatcher())
				// partition by cell id and event type
				.keyBy(0, 1)
				// build sliding window
				.timeWindow(Time.minutes(15), Time.minutes(5))
				// count ride events in window
				.apply(new RideCounter())
				// filter by popularity threshold
				.filter(new FilterFunction<Tuple4<Integer, Long, Boolean, Integer>>() {
					@Override
					public boolean filter(Tuple4<Integer, Long, Boolean, Integer> count) throws Exception {
						return count.f3 >= POPULAR_THRESHOLD;
					}
				})
				// map grid cell to coordinates
				.map(new GridToCoordinates());

		popularPlaces.print();

		// run the cleansing pipeline
		env.execute("Taxi Ride with Kafka");
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

	/**
	 * Maps taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

		@Override
		public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
			return new Tuple2<>(
					GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat),
					taxiRide.isStart
			);
		}
	}

	/**
	 * Counts the number of rides arriving or departing.
	 */
	public static class RideCounter implements WindowFunction<
			Tuple2<Integer, Boolean>,                // input type
			Tuple4<Integer, Long, Boolean, Integer>, // output type
			Tuple,                                   // key type
			TimeWindow>                              // window type
	{

		@SuppressWarnings("unchecked")
		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple2<Integer, Boolean>> gridCells,
				Collector<Tuple4<Integer, Long, Boolean, Integer>> out) throws Exception {

			int cellId = ((Tuple2<Integer, Boolean>)key).f0;
			boolean isStart = ((Tuple2<Integer, Boolean>)key).f1;
			long windowTime = window.getEnd();

			int cnt = 0;
			for(Tuple2<Integer, Boolean> c : gridCells) {
				cnt += 1;
			}

			out.collect(new Tuple4<>(cellId, windowTime, isStart, cnt));
		}
	}

	/**
	 * Maps the grid cell id back to longitude and latitude coordinates.
	 */
	public static class GridToCoordinates implements
			MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {

		@Override
		public Tuple5<Float, Float, Long, Boolean, Integer> map(
				Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {

			return new Tuple5<>(
					GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0),
					cellCount.f1,
					cellCount.f2,
					cellCount.f3);
		}
	}

}
