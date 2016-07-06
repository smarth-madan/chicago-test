package com.smadan.chicago;

import com.google.common.primitives.Ints;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.ChicagoTSClient;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by root on 6/23/16.
 */
public class WriteTest implements Runnable {
  private final static String key = "writeTestKey";
  ChicagoTSClient cts;
  private final CountDownLatch latch;
  private static AtomicInteger success = new AtomicInteger(0);
  private static AtomicInteger failure = new AtomicInteger(0);
  private static AtomicInteger readSuccess = new AtomicInteger(0);
  private static AtomicInteger readFailure = new AtomicInteger(0);
  private static Integer[] keys;
  int valCount;

  public WriteTest(CountDownLatch latch,ChicagoTSClient cts, int valCount){
    this.latch = latch;
    this.cts=cts;
    this.valCount=valCount;
  }

  @Override
  public void run(){
    try{
      String v = "val"+valCount+"                                                               "
          + "                                                                                   "
          + "                                                                                   end";
      byte[] val = v.getBytes();
      System.arraycopy(v.getBytes(),0,val,0,v.getBytes().length);
      int o =Ints.fromByteArray(cts.write(key.getBytes(),val));
      keys[valCount] =o;
      if(o%1000 == 0){
        System.out.println(o);
      }
      success.getAndIncrement();
    }catch (Exception e){
      failure.getAndIncrement();
      e.printStackTrace();
    }finally{
      latch.countDown();
    }
  }

  public static void main(String[] args) throws Exception {

    final int loop = Integer.parseInt(args[0]);
    final int workerSize = Integer.parseInt(args[1]);
    final int clients = Integer.parseInt(args[2]);
    ExecutorService executor = Executors.newFixedThreadPool(workerSize);
    CountDownLatch latch = new CountDownLatch(loop);
    ChicagoTSClient[] ctsa = new ChicagoTSClient[clients];
    keys = new Integer[loop];
    for(int i =0;i<clients;i++){
      ctsa[i] = new ChicagoTSClient("10.24.25.188:2181,10.24.25.189:2181,10.24.33.123:2181,10.25.145.56:2181",3);
      ctsa[i].startAndWaitForNodes(3);
    }

    System.out.println("########       Statring writes        #########");
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      executor.submit(new WriteTest(latch,ctsa[i%clients],i));
    }
    latch.await();
    System.out.println("Total time taken for "+loop+ " writes ="+ (System.currentTimeMillis() - startTime) + "ms");
    System.out.println("Total success :"+ success.get() + " Failures :"+ failure.get());
    System.out.println("########       Writes completed       #########");
    System.out.println();
    System.out.println();

    System.out.println("########       Verifying the writes        #########");
    System.out.println("Randomly reading 5 values");
    Random ran = new Random();
    for(int i =0;i<5;i++){

      int curkey = keys[ran.nextInt(loop)];
        try{
          String returnVal = new String(ctsa[curkey%clients].read(key.getBytes(),Ints.toByteArray(curkey)).get());
          System.out.println(curkey +" :"+returnVal);
          if(returnVal.startsWith("val")){
            readSuccess.getAndIncrement();
          }else{
            readFailure.getAndIncrement();
          }
        }catch(Exception e){
          e.printStackTrace();
          readFailure.getAndIncrement();
        }finally {
        }
    }
    System.out.println("Total read success :"+ readSuccess.get() + " Failures :"+ readFailure.get());
    executor.shutdownNow();

    System.exit(0);
  }
}
