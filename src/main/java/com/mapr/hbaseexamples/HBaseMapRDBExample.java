package com.mapr.hbaseexamples;

import com.mapr.hbaseexamples.entity.Employee;
import com.mapr.hbaseexamples.utils.RandomGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseMapRDBExample {

  public static void main(String[] args) throws IOException {
    Configuration hbaseConfig = createHBaseConfig("/opt/mapr/hbase/hbase-1.1.13/conf/hbase-site.xml");
    String tableNameHBase = "employee";
    String tableNameMapRDB = "/employee";

    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig))  {
      Admin admin = connection.getAdmin();
      createTableIfNotExists(admin, tableNameHBase);
      createTableIfNotExists(admin, tableNameMapRDB);

      insertRandomDataIntoTable(connection, tableNameHBase);

      copyTableData(connection, tableNameHBase, tableNameMapRDB);

      scanTable(connection, tableNameMapRDB);
    }
  }

  public static Configuration createHBaseConfig(String hbaseSiteLocation) {
    Configuration conf = new Configuration();
    conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
    conf.addResource(new Path(hbaseSiteLocation));

    return HBaseConfiguration.create(conf);
  }

  public static void createTableIfNotExists(Admin admin, String tableName) throws IOException {
    TableName tableNameObj = TableName.valueOf(tableName);
    if (!admin.tableExists(tableNameObj)) {
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));
      admin.createTable(tableDescriptor);
    }
  }

  public static void insertRandomDataIntoTable(Connection connection, String tableName) throws IOException {
    try (Table table = connection.getTable(TableName.valueOf(tableName))) {
      for (int i = 1; i < 10000; i++) {
        table.put(createPut(RandomGenerator.createEmployee(i)));
      }
    }
  }

  public static void copyTableData(Connection connection, String srcTableName, String destTableName)
      throws IOException {
    try (Table srcTable = connection.getTable(TableName.valueOf(srcTableName));
        Table destTable = connection.getTable(TableName.valueOf(destTableName));
        ResultScanner scanner = srcTable.getScanner(createScan())) {

        for (Result result = scanner.next(); result != null; result = scanner.next()) {
          destTable.put(createPut(result));
        }
    }

  }

  public static void scanTable(Connection connection, String tableName)
      throws IOException {
    try (Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(createScan())) {

      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        for(Cell cell : result.listCells()) {
          System.out.println(cell);
        }
      }
    }
  }

  public static Put createPut(Employee employee) {
    Put put = new Put(Bytes.toBytes(Integer.toString(employee.getId())));

    put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("position"), Bytes.toBytes(employee.getPosition()));
    put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"),
        Bytes.toBytes(Integer.toString(employee.getSalary())));
    put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("country"), Bytes.toBytes(employee.getCountry()));

    return put;
  }

  public static Put createPut(Result result) {
    Put put = new Put(result.getRow());

    for (Cell cell : result.listCells()) {
      byte[] bytes = cell.getRowArray();
      byte[] family = Bytes.copy(bytes, cell.getFamilyOffset(), cell.getFamilyLength());
      byte[] qualifier = Bytes.copy(bytes, cell.getQualifierOffset(), cell.getQualifierLength());
      byte[] value = Bytes.copy(bytes, cell.getValueOffset(), cell.getValueLength());
      put.addColumn(family, qualifier, cell.getTimestamp(), value);
    }

    return put;
  }

  public static Scan createScan() {
    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("position"));
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"));
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("country"));
    return scan;
  }

}
