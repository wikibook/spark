package com.wikibooks.spark.ch5;

public class Product {
  private String store;
  private String product;
  private Integer amount;
  private Integer price;

  public Product(String store, String product, Integer amount, Integer price) {
    this.store = store;
    this.product = product;
    this.amount = amount;
    this.price = price;
  }

  public String getStore() {
    return store;
  }

  public void setStore(String store) {
    this.store = store;
  }

  public String getProduct() {
    return product;
  }

  public void setProduct(String product) {
    this.product = product;
  }

  public Integer getAmount() {
    return amount;
  }

  public void setAmount(Integer amount) {
    this.amount = amount;
  }

  public Integer getPrice() {
    return price;
  }

  public void setPrice(Integer price) {
    this.price = price;
  }
}