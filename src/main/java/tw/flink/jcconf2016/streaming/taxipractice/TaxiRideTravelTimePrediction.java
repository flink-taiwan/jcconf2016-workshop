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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import tw.flink.jcconf2016.streaming.taxipractice.answer.TaxiRideTravelTimePredictionAnswer;
import tw.flink.jcconf2016.streaming.taxipractice.datamodel.TaxiRide;
import tw.flink.jcconf2016.streaming.taxipractice.source.CheckpointedTaxiRideSource;
import tw.flink.jcconf2016.streaming.taxipractice.util.GeoUtils;
import tw.flink.jcconf2016.streaming.taxipractice.util.TravelTimePredictionModel;

/**
 * ------------------------------------------------------------------------
 *                   EXERCISE #4: Taxi Ride Travel Time Prediction
 * ------------------------------------------------------------------------
 *
 * INSTRUCTIONS:
 *		The task is to predict the duration of each taxi ride. The prediction
 *		is made with a simple regression model, that is incrementally updated
 *		and trained as we receive ride events. When we receive an "end" event,
 *		we use it to update the model. When we receive a "start" event, we should
 *		output our prediction. Note that since this is an incrementally updated
 *		model that we want to persist even on failures, you MUST enable
 *		checkpointing!
 *
 * HINTS:
 *		There's already a class for the prediction model: {@link TravelTimePredictionModel}.
 *		When you get an "end" event, call {@link TravelTimePredictionModel#refineModel(int direction, double distance, double travelTime)}
 *		to refine the model. When you get a "start" event, call {@link TravelTimePredictionModel#predictTravelTime(int direction, double distance)}
 *		to predict the travel time for the ride.
 *
 * PARAMETERS:
 * 		nycTaxiRidesPath - absolute path to the nycTaxiRides.gz file
 *
 * REFERENCE ANSWER:
 * 		{@link TaxiRideTravelTimePredictionAnswer}
 */
public class TaxiRideTravelTimePrediction {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String nycTaxiRidesPath = params.getRequired("nycTaxiRidesPath");

		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// ===============================================================================
		//   1. we want to persist our incrementally trained model, even on failure,
		//      so you must enable checkpointing! (set to a 5 second interval)
		// ===============================================================================

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new CheckpointedTaxiRideSource(nycTaxiRidesPath, servingSpeedFactor));

		DataStream<Tuple2<Long, Integer>> predictions = rides
				// filter out rides that do not start or stop in NYC
				.filter(new NYCFilter())
				// map taxi ride events to the grid cell of the destination
				.map(new DestinationGridCellMatcher())
				// organize stream by destination
				.keyBy(0)
				// ===============================================================================
				//   2. the PredictionModel flatMap function is not implemented yet;
				//      finish its implementation down below!
				// ===============================================================================
				.flatMap(new PredictionModel());

		// print the predictions
		predictions.print();

		// run the prediction pipeline
		env.execute("Taxi Ride Prediction");
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

	/**
	 * Maps the taxi ride event to the grid cell of the destination location.
	 */
	public static class DestinationGridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, TaxiRide>> {

		@Override
		public Tuple2<Integer, TaxiRide> map(TaxiRide ride) throws Exception {
			int endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);

			return new Tuple2<>(endCell, ride);
		}
	}

	/**
	 * Predicts the travel time for taxi ride start events based on distance and direction.
	 * Incrementally trains a regression model using taxi ride end events.
	 */
	public static class PredictionModel extends RichFlatMapFunction<Tuple2<Integer, TaxiRide>, Tuple2<Long, Integer>> {

		// the prediction model; this is the state to update and checkpoint to Flink
		private transient ValueState<TravelTimePredictionModel> modelState;

		@Override
		public void flatMap(Tuple2<Integer, TaxiRide> val, Collector<Tuple2<Long, Integer>> out) throws Exception {

			// ===============================================================================
			//   3. When you get an "end" event, call
			//      TravelTimePredictionModel#refineModel(int direction, double distance, double travelTime)}
 			//      to refine the model. When you get a "start" event, call
			//      TravelTimePredictionModel#predictTravelTime(int direction, double distance)}
 			//      to predict the travel time for the ride and output the prediction.
			// ===============================================================================

		}

		@Override
		public void open(Configuration config) {

			// ===============================================================================
			//   4. When this operator is initialized, you should register and initialize
			//      modelState to be checkpointed by supplying a value state descriptor
			//      to the state backend (through the runtimeContext)
			// ===============================================================================

		}
	}

}
