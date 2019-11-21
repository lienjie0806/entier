package com.entier.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDemo {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        if (!(args.length == 1)) {
            System.out.println("please input one argument<create|list|insert|scan>");
            System.exit(-1);
        }

        String str = args[0];
        switch (str) {
            case "create":
                createTable("update_test", new String[]{"cf1", "cf2"});
                break;

            case "list":
                listTables();
                break;

            case "insert":
                insertRow("update_test", "20191105110000","cf1", "test", "{name: tom, age: 3}");
                insertRow("update_test", "20191105110000","cf1", "test2", "{name: jerry, age: 5}");
                insertRow("update_test", "20191105120000","cf2", "test2", "{name: jerry, age: 5}");
                break;

            case "scan":
                scanData("update_test", "20191105110000", "20191105110000");
                break;

            default:
                listTables();
                break;
        }
//
    }

    //初始化连接
    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "134.96.33.134,134.96.179.35,134.96.179.37");
        configuration.set("hbase.zookeeper.property.clientPort", "11001");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    public static  void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //建表
    public static void createTable(String tableName, String[] colFamiles) throws IOException {
        init();
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            System.out.println("talbe is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tName);
            for (String colFamily : colFamiles) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }

    //查看已有表
    public static void listTables() throws IOException {
        init();
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

//    //删表
//    public static void deleteTable(String tableName) throws IOException {
//        init();
//        TableName tName = TableName.valueOf(tableName);
//        if (admin.tableExists(tName)) {
//            admin.disableTable(tName);
//            admin.disableTable(tName);
//        }
//        close();
//    }

    //插入数据
    public static void insertRow(String tableName, String rowKey, String colFamily, String column, String value) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
        close();
    }

    //通过rowKey查找数据
    public static void getValue(String tableName, String rowKey, String colFamily, String column) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        showCell(result);
        close();
    }

    //批量查找数据
    public static void scanData(String tableName, String startRow, String endRow) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            showCell(result);
        }
        close();
    }

    //格式化输出
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("column Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    //删除指定列数据
    public static void deleteRow(String tableName, String rowKey, String colFamily, String column) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
//        delete.addFamily(Bytes.toBytes(colFamily));colFamily
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column));
        table.delete(delete);
        table.close();
        close();
    }
}
