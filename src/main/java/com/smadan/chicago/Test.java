package com.smadan.chicago;

import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.client.ChicagoClient;

/**
 * Created by root on 6/23/16.
 */
public class Test {
  public static void main(String[] args) throws Exception{
    ChicagoClient cc = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.24.33.123:2181,10.25.145.56:2181",3);
    cc.startAndWaitForNodes(3);
    System.out.println(new String(cc.stream("writeTestKey".getBytes(), Longs.toByteArray(13)).get().get(0)));

    ChicagoClient cc2 = new ChicagoClient("10.24.25.189:12000");
    System.out.println(new String(cc2.read("writeTestKey".getBytes(), Longs.toByteArray(5)).get().get(0)));
    System.exit(0);
  }
}
