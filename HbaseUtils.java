package com.sunny.RDD_key;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.util.*;

/**
 * DDL：
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 * <p>
 * DML：
 * 5.插入数据
 * 6.查数据（get）
 * 7.查数据（scan）
 * 8.删除数据
 */
public class HbaseUtils {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            //1.获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

            //2.创建连接对象
            connection = ConnectionFactory.createConnection(configuration);

            //3.创建Admin对象
            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //1.判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {

//        //1.获取配置文件信息
////        HBaseConfiguration configuration = new HBaseConfiguration();
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
//
//        //2.获取管理员对象
////        HBaseAdmin admin = new HBaseAdmin(configuration);
//        Connection connection = ConnectionFactory.createConnection(configuration);
//        Admin admin = connection.getAdmin();

        //3.判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //4.关闭连接
//        admin.close();

        //5.返回结果
        return exists;
    }

    //2.创建表
    public static void createTable(String tableName, String... cfs) throws IOException {

        //1.判断是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }

        //2.判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！");
            return;
        }

        //3.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //4.循环添加列族信息
        for (String cf : cfs) {

            //5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            //6.添加具体的列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);

        }

        //7.创建表
        admin.createTable(hTableDescriptor);
    }

    //3.删除表
    public static void dropTable(String tableName) throws IOException {

        //1.判断表是否存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在！！！");
            return;
        }

        //2.使表下线
        admin.disableTable(TableName.valueOf(tableName));

        //3.删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    //4.创建命名空间
    public static void createNameSpace(String ns) {

        //1.创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

        //2.创建命令空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(ns + "命名空间已存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("哈哈哈，尽管存在，我还是可以走到这！！");
    }

    //5.向表插入数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.给Put对象赋值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("male"));

        //4.插入数据
        table.put(put);

        //5.关闭表连接
        table.close();
    }

    //6.获取数据（get）
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //2.1.指定获取的列族
//        get.addFamily(Bytes.toBytes(cf));

        //2.2.指定列族和列
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //2.3.设置获取数据的版本数
        get.setMaxVersions(5);

        //3.获取数据
        Result result = table.get(get);
        System.out.println(result);

        //4.解析result并打印
        System.out.println(result.rawCells().length);
        for (Cell cell : result.rawCells()) {

            //5.打印数据
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    "，CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //6.关闭表连接
//        table.close();
    }

    public static HashMap<String, HashMap<String, String>> queryTableTestBatch(List<String> rowkeyList) throws IOException {
        List<Get> getList = new ArrayList();
        HashMap<String, HashMap<String, String>> resultMap = new HashMap<String, HashMap<String, String>>();
        String tableName = "student";
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        for (String rowkey : rowkeyList) {//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        System.out.println("results.length"+results.length);
        for (Result result : results) {//对返回的结果集进行操作
            System.out.println(result.rawCells().length);
            if(result.rawCells().length>0){
                String rowkey=Bytes.toString(result.getRow());
                HashMap<String, String> map = new HashMap<String, String>();
                for (Cell kv : result.rawCells()) {
                    String value = Bytes.toString(CellUtil.cloneValue(kv));
                    map.put(Bytes.toString(CellUtil.cloneQualifier(kv)),value);
                }
                resultMap.put(rowkey,map);
            }
        }
        table.close();
        return resultMap;
    }
        //7.获取数据（Scan）
    public static HashMap<String, HashMap<String, String>> scanTable(String tableName) throws IOException {

        HashMap<String, HashMap<String, String>> resultMap = new HashMap<String, HashMap<String, String>>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.构建Scan对象
        Scan scan = new Scan();

        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析resultScanner
        for (Result result : resultScanner) {

            HashMap<String, String> rowMap = new HashMap<String, String>();
            String rowkey=Bytes.toString(result.getRow());
            //5.解析result并打印
            for (Cell cell : result.rawCells()) {
                rowMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)),Bytes.toString(CellUtil.cloneValue(cell)));
                //6.打印数据
//                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
//                        "，CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
//                        "，CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
//                        "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));

            }
            resultMap.put(rowkey,rowMap);

        }

        //7.关闭连接
        table.close();
        return resultMap;
    }

    //8.删除数据
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //2.1.设置删除的列
//        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), 1563502276059L);

        //2.2.删除指定的列族
        delete.addFamily(Bytes.toBytes(cf));

        //3.执行删除操作
        table.delete(delete);

        //4.关闭连接
        table.close();

    }

    //关闭资源
    public static void close() {

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void writeTable(String tableName,HashMap<String,HashMap<String,String>> inputMap) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> list = new ArrayList<Put>();
        for(String rowkey :inputMap.keySet()){
            Put put = new Put(Bytes.toBytes(rowkey));
            for(Map.Entry<String,String> entry:inputMap.get(rowkey).entrySet()){
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
            list.add(put);
        }
        //4.插入数据
        table.put(list);

        //5.关闭表连接
        table.close();



    }


    public static void main(String[] args) throws IOException {

        //1.测试表是否存在
//        System.out.println(isTableExist("stu5"));

        //2.创建表测试
//        createTable("0408:stu5", "info1", "info2");

        //3.删除表测试
//        dropTable("stu5");

        //4.创建命名空间测试
//        createNameSpace("0408");

        //5.创建数据测试
//        putData("stu", "1002", "info2", "name", "zhangsan");

        //6.获取单行数据
//        getData("student", "1001", "info", "name");
//        List<String> list2 = new ArrayList<String>();
//        list2.add("1001");
//        list2.add("1002");
//
//        HashMap<String, HashMap<String, String>> a = queryTableTestBatch(list2);
//        System.out.println(a);
//        for (String i : a.keySet()){
//            System.out.println(i);
//            Set<Map.Entry<String, String>> entries = a.get(i).entrySet();
//            System.out.println(entries);
//
//        }



        //7.测试扫描数据
//        scanTable("stu");

        //8.测试删除
//        deleteData("stu", "1009", "info1", "name");

//        System.out.println(isTableExist("stu5"));

        //关闭资源
//        close();
        HashMap<String, HashMap<String, String>> map = new HashMap<String, HashMap<String, String>>();

        HashMap<String, String> map2 = new HashMap<String, String>();
        map2.put("name", "xijinping");
        map2.put("age", "65");
        map2.put("sex", "male");
        map.put("1003", map2);

        writeTable("student", map);

    }

}
