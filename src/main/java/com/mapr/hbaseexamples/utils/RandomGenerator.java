package com.mapr.hbaseexamples.utils;

import com.mapr.hbaseexamples.entity.Employee;

import java.util.Random;

public class RandomGenerator {

  private static String[] positions = { "Developer", "Manager", "SEO", "Accountant", "Lawyer" };
  private static String[] countries = { "Ukraine", "United States", "United Kingdom", "Italy" };
  private static Random random = new Random();

  public static Employee createEmployee(int id) {
    String position = positions[Math.abs(random.nextInt() % 5)];
    String country = countries[Math.abs(random.nextInt() % 4)];
    int salary = random.nextInt(50000);
    return new Employee(id, country, position, salary);
  }

}
