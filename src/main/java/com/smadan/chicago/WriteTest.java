package com.smadan.chicago;

import com.google.common.primitives.Ints;
import java.net.InetAddress;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 6/23/16.
 */
public class WriteTest {
  TestChicagoCluster testChicagoCluster;
  HashMap<String,String> servers = new HashMap<>();
  private final String key = "writeTestKey";

  @Before
  public  void  setup() throws  Exception{
    servers.put("s1","10.24.25.188:12000");
    servers.put("s2","10.24.33.123:12000");
    servers.put("s3","10.24.25.189:12000");
    servers.put("s4","10.25.145.56:12000");
    String zkString = "10.24.25.188:2181,10.24.25.189:2181,10.24.33.123:2181,10.25.145.56:2181";
    testChicagoCluster = new TestChicagoCluster(servers,zkString,3);
  }

  @Test
  public void write10Values() throws Exception{
    for (int i = 0; i < 10; i++) {
      String _v = InetAddress.getLocalHost().getHostAddress() + "val" + i;
      byte[] val = _v.getBytes();
      assertTrue(Ints.fromByteArray(testChicagoCluster.chicagoTSClient.write(key.getBytes(), val)) > 0);
    }
  }
}
