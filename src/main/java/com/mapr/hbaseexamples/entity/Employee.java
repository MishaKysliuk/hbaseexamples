package com.mapr.hbaseexamples.entity;

public class Employee {

  private int id;
  private String country;

  private String position;
  private int salary;

  public Employee(int id, String country, String position, int salary) {
    this.id = id;
    this.country = country;
    this.position = position;
    this.salary = salary;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public void setPosition(String position) {
    this.position = position;
  }

  public void setSalary(int salary) {
    this.salary = salary;
  }

  public int getId() {
    return id;
  }

  public String getCountry() {
    return country;
  }

  public String getPosition() {
    return position;
  }

  public int getSalary() {
    return salary;
  }
}
