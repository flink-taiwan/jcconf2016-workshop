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
package tw.flink.jcconf2016.streaming.taxipractice.datamodel;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * --------------- NOTE OF CODE ORIGIN FROM DATA-ARTISANS.COM ---------------
 * A duplicate of dataArtisans's implementation (https://github.com/dataArtisans/flink-training-exercises, ASL License).
 * --------------------------------------------------------------------------
 *
 * Implements a SerializationSchema and DeserializationSchema for TaxiRide for Kafka data sources and sinks.
 */
public class TaxiRideSchema implements DeserializationSchema<TaxiRide>, SerializationSchema<TaxiRide> {

	@Override
	public byte[] serialize(TaxiRide element) {
		return element.toString().getBytes();
	}

	@Override
	public TaxiRide deserialize(byte[] message) {
		return TaxiRide.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(TaxiRide nextElement) {
		return false;
	}

	@Override
	public TypeInformation<TaxiRide> getProducedType() {
		return TypeExtractor.getForClass(TaxiRide.class);
	}
}
