package com.meetme.venkat.kafka_strems_explore.model;

import lombok.Data;

@Data
public class Address {

    private String street;
    private String city;
    private String state;
    private Integer zipcode;
}
