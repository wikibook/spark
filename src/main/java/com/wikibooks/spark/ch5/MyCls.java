package com.wikibooks.spark.ch5;

import java.util.Map;

public class MyCls {
  private String id;
  private Map<String, String> value;

  public MyCls(String id, Map<String, String> value) {
    this.id = id;
    this.value = value;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Map<String, String> getValue() {
    return value;
  }

  public void setValue(Map<String, String> value) {
    this.value = value;
  }
}
