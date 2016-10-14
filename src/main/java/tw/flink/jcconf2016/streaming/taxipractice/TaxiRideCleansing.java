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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tw.flink.jcconf2016.streaming.taxipractice.answer.TaxiRideCleansingAnswer;
import tw.flink.jcconf2016.streaming.taxipractice.datamodel.TaxiRide;
import tw.flink.jcconf2016.streaming.taxipractice.source.TaxiRideSource;
import tw.flink.jcconf2016.streaming.taxipractice.util.GeoUtils;

/**
 * ------------------------------------------------------------------------
 * 		             EXERCISE #1: Taxi Ride Cleansing
 * ------------------------------------------------------------------------
 *
 * INSTRUCTIONS:
 *		The input stream of NYC taxi rides might contain events that have
 *		invalid geo coordinates. The task is to filter out events that have
 *		invliad geo coordinates and output the cleansed stream.
 *
 * HINTS:
 *		You can use {@link GeoUtils#isInNYC(float lon, float lat)} to check
 *		if a set of geo coordinates is in New York City.
 *
 * PARAMETERS:
 * 		nycTaxiRidesPath - absolute path to the nycTaxiRides.gz file
 *
 * REFERENCE ANSWER:
 * 		{@link TaxiRideCleansingAnswer}
 */
public class TaxiRideCleansing {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String nycTaxiRidesPath = params.get("nycTaxiRidesPath");

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource(nycTaxiRidesPath, maxEventDelay, servingSpeedFactor));

		// ===============================================================================
		//		1. clean up `rides`, so that the output stream only contains events
		//		   with valid geo coordinates within NYC.
		//		2. print out the result stream to console
		// ===============================================================================

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
	}

}
