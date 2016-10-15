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
package tw.flink.jcconf2016.streaming.taxipractice.answer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import tw.flink.jcconf2016.streaming.taxipractice.datamodel.TaxiRide;
import tw.flink.jcconf2016.streaming.taxipractice.source.TaxiRideSource;
import tw.flink.jcconf2016.streaming.taxipractice.util.GeoUtils;

/**
 * --------------- NOTE OF CODE ORIGIN FROM DATA-ARTISANS.COM ---------------
 * A duplicate of dataArtisans's implementation (https://github.com/dataArtisans/flink-training-exercises, ASL License)
 * --------------------------------------------------------------------------
 */
public class TaxiRidePopularPlacesAnswer {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String nycTaxiRidesPath = params.getRequired("nycTaxiRidesPath");

		final int popThreshold = 20;        // threshold for popular places
		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// ===============================================================================
		//   1. remember to set this job to use "Event Time"
		// ===============================================================================
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource(nycTaxiRidesPath, maxEventDelay, servingSpeedFactor));

		// ===============================================================================
		//   2. again, filter ride events to contain only valid geo coordinates
		//   3. map each ride event to a tuple 2 pair: (grid cell id, the event)
		//   4. partition the stream by the grid cell id
		//   5. aggregate the number of ride events (start and end) in each grid cell
		//      over a sliding window (span 15 minutes, slide 1 minute), and output:
		//      (cellId, time, eventCount)
		//   6. filter out window outputs if the number of ride events is
		//      lower than POPULAR_THRESHOLD.
		//   7. map the grid cell back to geo coordinates, and print as format:
		//      (lon, lat, time, eventCount)
		// ===============================================================================
		DataStream<Tuple4<Float, Float, Long, Integer>> popularSpots = rides
				// 2. again, filter ride events to contain only valid geo coordinates
				.filter(new NYCFilter())
				// 3. map each ride event to a tuple 2 pair: (grid cell id, the event)
				.map(new GridCellMatcher())
				// 4. partition the stream by the grid cell id
				.keyBy(0)
				// 5. aggregate the number of ride events (start and end) in each grid cell
				//	  over a sliding window (span 15 minutes, slide 1 minute), and output: (cellId, time, eventCount)
				.timeWindow(Time.minutes(15), Time.minutes(5))
				.apply(new RideCounter())
				// 6. filter out window outputs if the number of ride events is
				// 	  lower than POPULAR_THRESHOLD.
				.filter(new FilterFunction<Tuple3<Integer, Long, Integer>>() {
					@Override
					public boolean filter(Tuple3<Integer, Long, Integer> count) throws Exception {
						return count.f2 >= popThreshold;
					}
				})
				// 7. map the grid cell back to geo coordinates, and print as format: (lon, lat, time, eventCount)
				.map(new GridToCoordinates());

		// print result on stdout
		popularSpots.print();

		// execute the transformation pipeline
		env.execute("Popular Places");
	}

	/**
	 * Map taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, TaxiRide>> {

		@Override
		public Tuple2<Integer, TaxiRide> map(TaxiRide taxiRide) throws Exception {
			if(taxiRide.isStart) {
				// return grid cell id for start location
				return new Tuple2<>(GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat), taxiRide);
			} else {
				// return grid cell id for end location
				return new Tuple2<>(GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat), taxiRide);
			}
		}
	}

	/**
	 * Counts the number of rides arriving or departing.
	 */
	public static class RideCounter implements WindowFunction<
				Tuple2<Integer, TaxiRide>, // input type
				Tuple3<Integer, Long, Integer>, // output type
				Tuple, // key type
				TimeWindow> // window type
	{

		@SuppressWarnings("unchecked")
		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple2<Integer, TaxiRide>> values,
				Collector<Tuple3<Integer, Long, Integer>> out) throws Exception {

			int cellId = ((Tuple1<Integer>)key).f0;
			long windowTime = window.getEnd();

			int cnt = 0;
			for(Tuple2<Integer, TaxiRide> v : values) {
				cnt += 1;
			}

			out.collect(new Tuple3<>(cellId, windowTime, cnt));
		}
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

	/**
	 * Maps the grid cell id back to longitude and latitude coordinates.
	 */
	public static class GridToCoordinates implements
			MapFunction<Tuple3<Integer, Long, Integer>, Tuple4<Float, Float, Long, Integer>> {

		@Override
		public Tuple4<Float, Float, Long, Integer> map(
				Tuple3<Integer, Long, Integer> cellCount) throws Exception {

			return new Tuple4<>(
					GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0),
					cellCount.f1,
					cellCount.f2);
		}
	}

}
