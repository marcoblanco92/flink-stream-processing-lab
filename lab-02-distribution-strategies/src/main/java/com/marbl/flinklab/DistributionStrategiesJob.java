package com.marbl.flinklab;

import com.marbl.flinklab.model.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * Demonstrate how different distribution strategies affect data routing between subtasks
 *
 * Case A - default (forward): source p=1 → map p=4
 *   Expected: all events processed by the same subtask
 *
 * Case B - rebalance(): source p=1 → rebalance() → map p=4
 *   Expected: events distributed round-robin, 2 events per subtask
 *
 * Case C - shuffle(): source p=1 → shuffle() → map p=4
 *   Expected: events distributed randomly across subtasks
 *
 * Print subtask name and sensor reading for each case
 ***/
public class DistributionStrategiesJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);

        long now = System.currentTimeMillis();

        DataStream<SensorReading> forward = env.fromData(
                        new SensorReading("sensor-1", 30.0, now),
                        new SensorReading("sensor-2", 45.0, now + 1000),
                        new SensorReading("sensor-3", 38.5, now + 2000),
                        new SensorReading("sensor-1", 41.2, now + 3000),
                        new SensorReading("sensor-2", 39.0, now + 4000),
                        new SensorReading("sensor-3", 42.7, now + 5000),
                        new SensorReading("sensor-1", 28.3, now + 6000),
                        new SensorReading("sensor-2", 44.1, now + 7000)
                )
                .setParallelism(1)
                .forward();

        DataStream<String> forwardStream = forward.map(r -> String.format("[FORWARD][%s] %s %.1f°C",
                        Thread.currentThread().getName(),
                        r.getSensorId(),
                        r.getTemperature()))
                .setParallelism(1);

        forwardStream.print();


        DataStream<SensorReading> rebalance = env.fromData(
                        new SensorReading("sensor-01", 30.0, now),
                        new SensorReading("sensor-02", 45.0, now + 1000),
                        new SensorReading("sensor-03", 38.5, now + 2000),
                        new SensorReading("sensor-01", 41.2, now + 3000),
                        new SensorReading("sensor-02", 39.0, now + 4000),
                        new SensorReading("sensor-03", 42.7, now + 5000),
                        new SensorReading("sensor-01", 28.3, now + 6000),
                        new SensorReading("sensor-02", 44.1, now + 7000)
                ).setParallelism(1)
                .rebalance();

        DataStream<String> rebalanceStream = rebalance.map(r -> String.format("[REBALANCE][%s] %s %.1f°C",
                        Thread.currentThread().getName(),
                        r.getSensorId(),
                        r.getTemperature()))
                .setParallelism(4);

        rebalanceStream.print();

        DataStream<SensorReading> shuffle = env.fromData(
                        new SensorReading("sensor-001", 30.0, now),
                        new SensorReading("sensor-002", 45.0, now + 1000),
                        new SensorReading("sensor-003", 38.5, now + 2000),
                        new SensorReading("sensor-001", 41.2, now + 3000),
                        new SensorReading("sensor-002", 39.0, now + 4000),
                        new SensorReading("sensor-003", 42.7, now + 5000),
                        new SensorReading("sensor-001", 28.3, now + 6000),
                        new SensorReading("sensor-002", 44.1, now + 7000)
                ).setParallelism(1)
                .shuffle();

        DataStream<String> shuffleStream = shuffle.map(r -> String.format("[SHUFFLE][%s] %s %.1f°C",
                        Thread.currentThread().getName(),
                        r.getSensorId(),
                        r.getTemperature()))
                .setParallelism(4);

        shuffleStream.print();

        env.execute();

    }
}
