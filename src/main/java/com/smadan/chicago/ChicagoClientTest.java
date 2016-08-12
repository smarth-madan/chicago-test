package com.smadan.chicago;

import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.client.ChicagoClientException;
import com.xjeffrose.chicago.client.ChicagoClientTimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 8/8/16.
 */
public class ChicagoClientTest {
  TestChicagoCluster testChicagoCluster;
  HashMap<String, String> servers = new HashMap<>();

  @Before
  public void setup() throws Exception {
    servers.put("10.24.25.188:12000", "10.24.25.188:12000");
    servers.put("10.24.33.123:12000", "10.24.33.123:12000");
    servers.put("10.24.25.189:12000", "10.24.25.189:12000");
    servers.put("10.25.145.56:12000", "10.25.145.56:12000");
    String zkString = "10.24.25.188:2181,10.24.25.189:2181,10.24.33.123:2181,10.25.145.56:2181";
    testChicagoCluster = new TestChicagoCluster(servers, zkString, 3);
  }

  public String forServer(String server) {
    String result = null;
    for (String k : servers.keySet()) {
      String s = servers.get(k);
      if (s.equals(server)) {
        result = k;
      }
    }
    return result;
  }

  @Test
  public void transactMany() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "xxkey" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertNotNull(testChicagoCluster.chicagoClient.write(key, val).get().get(0));
      assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read(key).get().get(0)));
    }
  }

  @Test
  public void transactManyCF() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      testChicagoCluster.chicagoClient.write("colfam".getBytes(), key, val);
      assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read("colfam".getBytes(), key).get().get(0)));
    }
  }

  @Test
  public void transactManyCFConcurrent() throws Exception {
    ExecutorService exe = Executors.newFixedThreadPool(6);
    int count = 500;
    CountDownLatch latch = new CountDownLatch(count * 3);


    exe.execute(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < count; i++) {
            String _k = "xkey" + i;
            byte[] key = _k.getBytes();
            String _v = "xval" + i;
            byte[] val = _v.getBytes();
            Assert.assertNotNull(testChicagoCluster.chicagoClient.write("xcolfam".getBytes(), key, val).get().get(0));
            assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read("xcolfam".getBytes(), key).get().get(0)));
            latch.countDown();
          }
        } catch (ChicagoClientTimeoutException e) {
          e.printStackTrace();
        } catch (ChicagoClientException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        } catch (TimeoutException e) {
          e.printStackTrace();
        }
      }
    });


    exe.execute(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < count; i++) {
            String _k = "ykey" + i;
            byte[] key = _k.getBytes();
            String _v = "yval" + i;
            byte[] val = _v.getBytes();
            assertNotNull(testChicagoCluster.chicagoClient.write("ycolfam".getBytes(), key, val).get().get(0));
            assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read("ycolfam".getBytes(), key).get().get(0)));
            latch.countDown();
          }
        } catch (ChicagoClientTimeoutException e) {
          e.printStackTrace();
        } catch (ChicagoClientException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        } catch (TimeoutException e) {
          e.printStackTrace();
        }
      }
    });


    exe.execute(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < count; i++) {
            String _k = "zkey" + i;
            byte[] key = _k.getBytes();
            String _v = "zval" + i;
            byte[] val = _v.getBytes();
            Assert.assertNotNull(testChicagoCluster.chicagoClient.write("xcolfam".getBytes(), key, val).get().get(0));

            assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read("xcolfam".getBytes(), key).get().get(0)));
            latch.countDown();
          }
        } catch (ChicagoClientTimeoutException e) {
          e.printStackTrace();
        } catch (ChicagoClientException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        } catch (TimeoutException e) {
          e.printStackTrace();
        }
      }
    });


    latch.await(20000, TimeUnit.MILLISECONDS);
    exe.shutdownNow();
  }


  @Test
  public void transactStream() throws Exception {
    Set<String> values = new HashSet<>();
    Set<String> resultValues = new HashSet<>();
    String colFam = "test-colFam";
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());

    for (int i = 0; i < 1000; i++) {
      String _v = "val" + i + "end!!"+i;
      byte[] val = _v.getBytes();
      values.add(_v);
      byte[] ret = testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(), val).get().get(0);
      assertTrue(Longs.fromByteArray(ret) >= 0 && Longs.fromByteArray(ret) < 1000);
    }

    String delimiter = ChiUtil.delimiter;

    byte[] resp = testChicagoCluster.chicagoClient.stream(colFam
      .getBytes(),Longs.toByteArray(0l)).get().get(0);

    assertNotNull(resp);
    byte[] resultArray = resp;
    long offset;
    String result = new String(resultArray);

    long old = -1;
    int count = 0;
    while (result.contains(delimiter)) {

      if (count > 10) {
        break;
      }
      offset = ChiUtil.findOffset(resultArray);
      if (old != -1 && (old == offset)) {
        Thread.sleep(500);
      }


      String output = result.split(ChiUtil.delimiter)[0];
      for(String line : output.split("\0")){
        resultValues.add(line);
      }

      if(!output.isEmpty()){
        offset = offset +1;
      }
      resp = testChicagoCluster.chicagoClient.stream(
        colFam.getBytes(), Longs.toByteArray(offset)).get().get(0);
      resultArray = resp;
      result = new String(resp);
      old = offset;
      count++;
    }

    values.removeAll(resultValues);
    assertTrue(values.isEmpty());
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
  }

  @Test
  public void transactLargeStream() throws Exception {
    byte[] value=null;
    String returnValString="someJunk";
    String colFam = "LargeTskey";
    int size=10240;
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());

    byte[] offset = null;
    for (int i = 0; i < 50; i++) {
      byte[] val = new byte[size];
      if (i == 12) {
        for(int j =0 ; j< size ; j++){
          val[j] = 10;
        }
        value = val;
        offset = testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(), val).get().get(0);
      }else {
        assertNotNull(
          testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(), val).get().get(0));
      }
    }

    byte[] _resp = testChicagoCluster.chicagoClient.stream(
      colFam.getBytes(), offset).get().get(0);
    String result = new String(_resp);
    assertTrue(result.contains(ChiUtil.delimiter));
    returnValString = result.split(ChiUtil.delimiter)[0].split("\0")[0];
    assertEquals(new String(value), returnValString);
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
  }

}
