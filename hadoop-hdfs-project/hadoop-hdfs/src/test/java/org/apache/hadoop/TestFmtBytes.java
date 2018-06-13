package org.apache.hadoop;

public class TestFmtBytes {
  public static void main(String[] args) {
    float v=Long.MAX_VALUE;
    char UNITS[] = new char []{'B', 'K', 'M', 'G', 'T', 'P', 'Z'};
    int i=0;
    float prev = 0;
    while (Math.floor(v) > 0 && i < UNITS.length) {
      prev = v;
      v /= 1024;
      i += 1;
    }

    if (i > 0 && i <= UNITS.length) {
      v = prev;
      i -= 1;
    }
    System.out.println(Math.round(v * 100) / 100 + " " + UNITS[i]);
  }
}
