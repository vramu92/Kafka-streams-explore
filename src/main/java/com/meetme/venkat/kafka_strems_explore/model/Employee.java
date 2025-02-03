package com.meetme.venkat.kafka_strems_explore.model;

import lombok.Data;

@Data
public class Employee {

    private String firstName;
    private String lastName;
    private String employeeId;
    private Address address;
    private PersonalInformation personalInformation;
}
