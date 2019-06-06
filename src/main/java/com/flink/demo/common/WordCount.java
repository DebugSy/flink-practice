package com.flink.demo.common;

/**
 * Created by DebugSy on 2019/5/22.
 */
public class WordCount {

  public String word;

  public long frequency;

  public WordCount() {
  }

  public WordCount(String word, long frequency) {
    this.word = word;
    this.frequency = frequency;
  }

  @Override
  public String toString() {
    return "WordCount[" + word + "," + frequency + "]";
  }
}
