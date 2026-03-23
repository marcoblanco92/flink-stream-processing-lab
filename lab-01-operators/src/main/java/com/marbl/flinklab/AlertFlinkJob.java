package com.marbl.flinklab;

import com.marbl.flinklab.model.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/***
 * Read a sequence of IoT sensor readings
 * Generate zero, one or two alerts based on temperature:
 *   - temp < 40°C          → no alert
 *   - 40°C <= temp < 45°C  → 1 alert: "WARNING: sensor-X at XX.X°C"
 *   - temp >= 45°C         → 2 alerts: "WARNING: ..." + "CRITICAL: sensor-X IMMEDIATE ACTION"
 * Print alerts to stdout
 ***/
public class AlertFlinkJob {
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

        DataStream<String> anomalies = stream.flatMap((SensorReading sensorReading, Collector<String> collector) -> {
            if (sensorReading.getTemperature() >= 40.0 && sensorReading.getTemperature() < 45.0)
                collector.collect(String.format("WARNING: %s at %.1f°C",
                        sensorReading.getSensorId(),
                        sensorReading.getTemperature()
                ));

            if (sensorReading.getTemperature() >= 45.0) {
                collector.collect(String.format("WARNING: %s at %.1f°C",
                        sensorReading.getSensorId(),
                        sensorReading.getTemperature()
                ));
                collector.collect(String.format("CRITICAL: %s IMMEDIATE ACTION",
                        sensorReading.getSensorId()
                ));
            }
        }).returns(String.class);

        anomalies.print();

        env.execute("01 Lab Hello Flink - Sensor Anomaly Detection");
    }
}
