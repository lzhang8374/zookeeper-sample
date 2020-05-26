package com.sample.zookeeper;

public class ZkTest {

    public static void main(String[] args) {
        ZkOper zkOper = new ZkOper();
        zkOper.connect("192.168.0.130:2181");


        String path = "/path9";

        zkOper.createNode(path, "aaaaaaaaa", true, true);
        System.out.println(zkOper.getNode(path));

//        zkOper.updateNode(path, "bbbbbbbbbb");
//        System.out.println(zkOper.getNode(path));

        //zkOper.delNode(path);

        zkOper.close();
        System.out.println("---------------------over----------------------");
    }
}
