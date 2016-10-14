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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tw.flink.jcconf2016.streaming.taxipractice.answer.TaxiRidePopularPlacesAnswer;
import tw.flink.jcconf2016.streaming.taxipractice.datamodel.TaxiRide;
import tw.flink.jcconf2016.streaming.taxipractice.source.TaxiRideSource;
import tw.flink.jcconf2016.streaming.taxipractice.util.GeoUtils;

/**
 * ------------------------------------------------------------------------
 *                   EXERCISE #2: Taxi Ride Popular Places
 * ------------------------------------------------------------------------
 *
 * INSTRUCTIONS:
 *		The task is to calculate popular places (places where people frequently
 *		take rides or get off rides) over a sliding window. You should group geo
 *		locations by the grid cell id when calculating popularity of a location.
 *
 * HINTS:
 *		You can use {@link GeoUtils#mapToGridCell(float lon, float lat)} to map
 *		a geo coordinate to its grid cell id. You can also use
 *		{@link GeoUtils#getGridCellCenterLon(int cellId)}
 *		and {@link GeoUtils#getGridCellCenterLat(int cellId)} to transform a grid
 *		cell id back to geo coordinates.
 *
 * PARAMETERS:
 * 		nycTaxiRidesPath - absolute path to the nycTaxiRides.gz file
 *
 * REFERENCE ANSWER:
 * 		{@link TaxiRidePopularPlacesAnswer}
 */
public class TaxiRidePopularPlaces {

	private static final int MAX_EVENT_DELAY = 60;			// events are out of order by max 60 seconds
	private static final int SERVING_SPEED_FACTOR = 600;	// events of 10 minute are served in 1 second

	private static final int POPULAR_THRESHOLD = 20; 		// threshold for popular places

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String nycTaxiRidesPath = params.getRequired("nycTaxiRidesPath");

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// ===============================================================================
		//   1. remember to set this job to use "Event Time"
		// ===============================================================================

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource(nycTaxiRidesPath, MAX_EVENT_DELAY, SERVING_SPEED_FACTOR));

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

		// execute the transformation pipeline
		env.execute("Popular Places");
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

}
