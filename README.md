# Flink Stream Processing Lab

Learning path based on "Stream Processing with Apache Flink" — Hueske & Kalavri (O'Reilly, 2019).
Flink version: 2.2.0 | Java: 21 | Build: Maven multi-module

## Modules

### lab-01-operators
**Topic:** Core DataStream API operators  
**Flink concepts:** filter, map, flatMap, keyBy, reduce  
**Jobs:**
- `SensorFlinkJob` — filter anomalies above 40°C, format output with map
- `AlertFlinkJob` — generate 0/1/2 alerts per reading using flatMap + Collector
- `SensorAggregationFlinkJob` — rolling max temperature per sensor using keyBy + reduce

### lab-02-distribution-strategies
**Topic:** Data distribution strategies between subtasks  
**Flink concepts:** forward, rebalance, shuffle, operator chaining  
**Jobs:**
- `DistributionStrategiesJob` — compare forward vs rebalance vs shuffle
  with source p=1 and map p=4, observing subtask assignment via thread name