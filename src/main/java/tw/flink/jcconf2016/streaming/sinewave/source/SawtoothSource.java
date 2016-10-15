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
package tw.flink.jcconf2016.streaming.sinewave.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import tw.flink.jcconf2016.streaming.sinewave.datamodel.UnkeyedDataPoint;

public class SawtoothSource extends RichSourceFunction<UnkeyedDataPoint> implements Checkpointed<Tuple2<Long, Integer>> {
	private final int intervalMillis;
	private final int numberOfSteps;
	private final int slowdownFactor;
	private volatile boolean running = true;

	// Checkpointed State
	private volatile long currentTimeMs = 0;
	private int currentStep = 0;

	public SawtoothSource(int intervalMillis, int numberOfSteps, int slowdownFactor){
		this.intervalMillis = intervalMillis;
		this.numberOfSteps = numberOfSteps;
		this.slowdownFactor = slowdownFactor;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		long now = System.currentTimeMillis();
		if(currentTimeMs == 0) {
			currentTimeMs = now - (now % 1000); // floor to second boundary
		}
	}

	@Override
	public void run(SourceContext<UnkeyedDataPoint> ctx) throws Exception {
		while (running) {
			synchronized (ctx.getCheckpointLock()) {
				double stepHeight = (double) currentStep / numberOfSteps;
				currentStep = ++currentStep % numberOfSteps;

				ctx.collectWithTimestamp(new UnkeyedDataPoint(currentTimeMs, stepHeight), currentTimeMs);
				ctx.emitWatermark(new Watermark(currentTimeMs));
				currentTimeMs += intervalMillis;
			}
			timeSync();
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	@Override
	public Tuple2<Long,Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		return new Tuple2<Long,Integer>(currentTimeMs, currentStep);
	}

	@Override
	public void restoreState(Tuple2<Long, Integer> restoredState) throws Exception {
		currentTimeMs = restoredState.f0;
		currentStep = restoredState.f1;
	}

	private void timeSync() throws InterruptedException {
		// Sync up with real time
		long realTimeDeltaMs = currentTimeMs - System.currentTimeMillis();
		long sleepTime = intervalMillis + realTimeDeltaMs + randomJitter();

		if(slowdownFactor != 1){
			sleepTime = intervalMillis * slowdownFactor;
		}

		if(sleepTime > 0) {
			Thread.sleep(sleepTime);
		}
	}

	private long randomJitter(){
		double sign = -1.0;
		if(Math.random() > 0.5){
			sign = 1.0;
		}
		return (long)(Math.random() * intervalMillis * sign);
	}
}
