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
package tw.flink.jcconf2016.streaming.sinewave.datamodel;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class UnKeyedDataPointSchema implements DeserializationSchema<UnkeyedDataPoint>, SerializationSchema<UnkeyedDataPoint> {

	@Override
	public byte[] serialize(UnkeyedDataPoint element) {
		return element.toString().getBytes();
	}

	@Override
	public UnkeyedDataPoint deserialize(byte[] message) {
		return UnkeyedDataPoint.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(UnkeyedDataPoint nextElement) {
		return false;
	}

	@Override
	public TypeInformation<UnkeyedDataPoint> getProducedType() {
		return TypeExtractor.getForClass(UnkeyedDataPoint.class);
	}

}
