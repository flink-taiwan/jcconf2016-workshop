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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import tw.flink.jcconf2016.streaming.taxipractice.datamodel.TaxiRide;
import tw.flink.jcconf2016.streaming.taxipractice.source.CheckpointedTaxiRideSource;
import tw.flink.jcconf2016.streaming.taxipractice.util.GeoUtils;
import tw.flink.jcconf2016.streaming.taxipractice.util.TravelTimePredictionModel;

public class TaxiRideTravelTimePredictionAnswer {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String nycTaxiRidesPath = params.getRequired("nycTaxiRidesPath");

		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// ===============================================================================
		//		1. we want to persist our incrementally trained model, even on failure,
		//		   so you must enable checkpointing! (set to a 5 second interval)
		// ===============================================================================
		env.enableCheckpointing(5000);

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
				//		2. the PredictionModel flatMap function is not implemented yet;
				//		   finish its implementation down below!
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

		private transient ValueState<TravelTimePredictionModel> modelState;

		@Override
		public void flatMap(Tuple2<Integer, TaxiRide> val, Collector<Tuple2<Long, Integer>> out) throws Exception {

			// ===============================================================================
			//		3. When you get an "end" event, call
			//         TravelTimePredictionModel#refineModel(int direction, double distance, double travelTime)}
			// 		   to refine the model. When you get a "start" event, call
			//         TravelTimePredictionModel#predictTravelTime(int direction, double distance)}
			// 		   to predict the travel time for the ride and output the prediction.
			// ===============================================================================

			// fetch operator state
			TravelTimePredictionModel model = modelState.value();

			TaxiRide ride = val.f1;
			// compute distance and direction
			double distance = GeoUtils.getEuclideanDistance(ride.startLon, ride.startLat, ride.endLon, ride.endLat);
			int direction = GeoUtils.getDirectionAngle(ride.endLon, ride.endLat, ride.startLon, ride.startLat);

			if(ride.isStart) {
				// we have a start event: Predict travel time
				int predictedTime = model.predictTravelTime(direction, distance);
				// emit prediction
				out.collect(new Tuple2<>(ride.rideId, predictedTime));
			} else {
				// we have an end event: Update model
				// compute travel time in minutes
				double travelTime = (ride.endTime.getMillis() - ride.startTime.getMillis()) / 60000.0;
				// refine model
				model.refineModel(direction, distance, travelTime);
				// update operator state
				modelState.update(model);
			}
		}

		@Override
		public void open(Configuration config) {

			// ===============================================================================
			//		4. When this operator is initialized, you should register and initialize
			// 		   modelState to be checkpointed by supplying a value state descriptor
			//		   to the state backend (through the runtimeContext)
			// ===============================================================================

			// obtain key-value state for prediction model
			ValueStateDescriptor<TravelTimePredictionModel> descriptor =
					new ValueStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(new TypeHint<TravelTimePredictionModel>() {}),
							// default value of state
							new TravelTimePredictionModel());
			modelState = getRuntimeContext().getState(descriptor);
		}
	}

}
