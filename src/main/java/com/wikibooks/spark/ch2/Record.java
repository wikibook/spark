package com.wikibooks.spark.ch2;

import java.io.Serializable;

public class Record implements Serializable {
  public long number;
  public long amount;

  public Record(long amount, long number) {
    this.amount = amount;
    this.number = number;
  }
  
  public Record(long amount) {
    this.amount = amount;
    this.number = 1;
  }  

  public Record add(long amount) {
    this.number += 1;
    this.amount += amount;
    return this;
  }

  public Record add(Record other) {
    this.number += other.number;
    this.amount += other.amount;
    return this;
  }

  public String toString() {
    return "avg:" + amount / number;
  }
}