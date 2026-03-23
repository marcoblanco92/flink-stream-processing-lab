package com.marbl.flinklab;


import com.marbl.flinklab.model.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * Read a sequence of IoT sensor readings
 * Group events by sensorId
 * Track the maximum temperature per sensor (rolling)
 * Print the updated maximum after each event
 ***/
public class SensorAggregationFlinkJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);

        long now = System.currentTimeMillis();

        DataStream<SensorReading> stream = env.fromData(
                new SensorReading("sensor-1", 30.0, now),
                new SensorReading("sensor-2", 45.0, now + 1000),
                new SensorReading("sensor-3", 38.5, now + 2000),
                new SensorReading("sensor-1", 41.2, now + 3000),
                new SensorReading("sensor-2", 39.0, now + 4000),
                new SensorReading("sensor-3", 42.7, now + 5000),
                new SensorReading("sensor-1", 28.3, now + 6000),
                new SensorReading("sensor-2", 44.1, now + 7000),
                new SensorReading("sensor-3", 36.6, now + 8000),
                new SensorReading("sensor-1", 47.5, now + 9000)
        );

        DataStream<SensorReading> anomalies = stream.keyBy(SensorReading::getSensorId)
                .reduce((sensorReadingAccumulator, sensorReading) -> {
                    if (sensorReadingAccumulator.getTemperature() > sensorReading.getTemperature()) {
                        return sensorReadingAccumulator;
                    } else {
                        return sensorReading;
                    }
                });

        anomalies.print();

        env.execute("01 Lab Hello Flink - Sensor Anomaly Detection");
    }
}
