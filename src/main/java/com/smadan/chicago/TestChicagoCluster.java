package com.smadan.chicago;

import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.ChicagoTSClient;

import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.ChicagoTSClient;

import java.util.HashMap;


/**
 * Created by smadan on 6/20/16.
 */
public class TestChicagoCluster {



    public ZkClient zkClient;
    public final HashMap<String, ChicagoClient> chicagoClientHashMap = new HashMap<>();
    public final HashMap<String, ChicagoTSClient> chicagoTSClientHashMap = new HashMap<>();
    public final ChicagoClient chicagoClient;
    public final ChicagoTSClient chicagoTSClient;
    public HashMap<String, String> servers;
    public final static String ELECTION_PATH = "/chicago/chicago-elect";
    public final static String NODE_LIST_PATH = "/chicago/node-list";
    public final static String NODE_LOCK_PATH = "/chicago/replication-lock";

    public TestChicagoCluster(HashMap<String,String> servers, String zkConnectString, int quorom) throws Exception{
        this.servers = servers;
        zkClient = new ZkClient(zkConnectString);
        zkClient.start();
        chicagoClient = new ChicagoClient(zkConnectString,quorom);
        chicagoTSClient = new ChicagoTSClient(zkConnectString,quorom);
        chicagoClient.startAndWaitForNodes(quorom);
        chicagoTSClient.startAndWaitForNodes(quorom);

        servers.keySet().forEach(k ->{
            try {
                String server = servers.get(k);
                ChicagoClient ccl = new ChicagoClient(server);
                ChicagoTSClient ccts = new ChicagoTSClient(server);
                chicagoClientHashMap.put(k,ccl);
                chicagoTSClientHashMap.put(k,ccts);
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    public void markNodeDown(String serverName){
        zkClient.delete(NODE_LIST_PATH+"/"+servers.get(serverName));
    }

    public void markNodeUp(String serverName){
        zkClient.createIfNotExist(NODE_LIST_PATH+"/"+servers.get(serverName),"");
    }

    public boolean checkIfNodeExists(String path){
        try {
            return (zkClient.getClient().checkExists().forPath(path) != null);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
