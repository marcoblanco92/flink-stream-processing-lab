package com.marbl.flinklab.model;

import lombok.*;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading implements Serializable {

    private String sensorId;
    private double temperature;
    private long timestamp;
}
