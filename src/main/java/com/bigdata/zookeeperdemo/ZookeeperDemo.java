package com.bigdata.zookeeperdemo;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperDemo {
    private static String connectString =
            "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

//    @Before
//    public void init() throws Exception {
//
//        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
//            @Override
//            public void process(WatchedEvent event) {
//                // 收到事件通知后的回调函数（用户的业务逻辑）
//                System.out.println("类型为:" + event.getType() + "  路径为:" + event.getPath());
//
//                /*// 再次启动监听
//                try {
//                    zkClient.getChildren("/", true);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }*/
//            }
//        });
//    }
    /**
     *
     *创建节点
     */
    @Test
    public void create() throws Exception {
        // 获取客户端
        zkClient = new ZooKeeper(connectString, sessionTimeout, null);

        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
        String nodeCreated = zkClient.create("/eclipse", "hello zookeeper".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(nodeCreated);
    }


    /**
     * 查看节点
     */
    @Test
    public void getChildren() throws Exception {
        // 获取客户端
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
               System.out.println("类型为:" + event.getType() + "  路径为:" + event.getPath());
                try {
                    zkClient.getChildren("/eclipse", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        List<String> children = zkClient.getChildren("/eclipse", true);
        for(String str : children){
            System.out.println(str);
        }
        // 延时阻塞
        Thread.sleep(1000000);
    }


    /**
     * 删除节点
     */
    @Test
    public void delete() throws Exception {
        // 获取客户端
        zkClient = new ZooKeeper(connectString, sessionTimeout, null);
        // delete(第二个参数代表要删除的版本号，指定的-1是无视版本)
        zkClient.delete("/a0000000010",-1);

    }

}
