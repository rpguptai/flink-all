package com.versh.transform

import com.versh.utils._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/** Object that defines the DataStream program in the main() method */
object KeyedTransformations {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // group sensor readings by their id
    val keyed: KeyedStream[SensorReading, String] = readings
      .keyBy(_.id)

    // a rolling reduce that computes the highest temperature of each sensor and
    // the corresponding timestamp
    val maxTempPerSensor: DataStream[SensorReading] = keyed
      .reduce((r1, r2) => {
        if (r1.temperature > r2.temperature) r1 else r2
      })

    maxTempPerSensor.print()

    // execute application
    env.execute("Keyed Transformations Example")
  }

}
