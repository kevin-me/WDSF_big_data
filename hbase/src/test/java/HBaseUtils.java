import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;

public class HBaseUtils
{
    private Connection connection;

    private final String TABLENAME = "user";

    private Table table;

    @Before
    public void initTable() throws IOException
    {

        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181");

        connection = ConnectionFactory.createConnection(configuration);

        table = connection.getTable(TableName.valueOf(TABLENAME));

    }

    @After
    public void close() throws IOException
    {

        table.close();

        connection.close();

    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws IOException
    {

        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLENAME));

        hTableDescriptor.addFamily(new HColumnDescriptor("f1"));

        hTableDescriptor.addFamily(new HColumnDescriptor("f2"));

        admin.createTable(hTableDescriptor);

        admin.createTable(hTableDescriptor);

        admin.close();

        connection.close();

    }

    /**
     * 插入数据
     */

    public void insertHbaseData() throws IOException
    {

        Put put = new Put("00001".getBytes());

        put.addColumn("f1".getBytes(), "name".getBytes(), "xiaoran".getBytes());

        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));

        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));

        put.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("地球人"));

        put.addColumn("f2".getBytes(), "angle".getBytes(), Bytes.toBytes("angle"));

        table.put(put);

        table.close();


    }

    /**
     * 查询数据
     */


    public void getHbaseData() throws IOException
    {

        Get get = new Get("00001".getBytes());

        get.addFamily("f1".getBytes());

        get.addColumn("f2".getBytes(), "name".getBytes());

        Result result = table.get(get);

        List<Cell> cells = result.listCells();


        if (cells != null)
        {
            for (Cell cell : cells)
            {
                byte[] family_name = CellUtil.cloneFamily(cell);

                byte[] column_name = CellUtil.cloneQualifier(cell);

                byte[] rowkey = CellUtil.cloneRow(cell);

                byte[] cell_value = CellUtil.cloneValue(cell);


                if ("age".equals(Bytes.toString(column_name)) || "id".equals(Bytes.toString(column_name)))
                {
                    System.out.print(Bytes.toString(family_name));
                    System.out.print(Bytes.toString(column_name));
                    System.out.print(Bytes.toString(rowkey));
                    System.out.print(Bytes.toInt(cell_value));
                }
                else
                {
                    System.out.print(Bytes.toString(family_name));
                    System.out.print(Bytes.toString(column_name));
                    System.out.print(Bytes.toString(rowkey));
                    System.out.print(Bytes.toString(cell_value));
                }
            }
        }

        table.close();
    }

    /**
     * 批量插入
     */

    public void patchInsertHbaseData() throws IOException
    {
        Put put1 = new Put("00001".getBytes());

        put1.addColumn("f1".getBytes(), "name".getBytes(), "sss".getBytes());

        Put put2 = new Put("00002".getBytes());

        put2.addColumn("f2".getBytes(), "name".getBytes(), "sss".getBytes());

        List<Put> putList = new ArrayList<>();

        putList.add(put1);

        putList.add(put2);

        table.put(putList);

        table.close();
    }

    /**
     * scan 数据
     */

    public void scanHbaseData() throws IOException
    {

        Scan scan = new Scan("00001".getBytes());

        scan.addFamily("f1".getBytes());

        scan.addColumn("f2".getBytes(), "name".getBytes());

        scan.setStartRow("0003".getBytes());

        scan.setStopRow("0007".getBytes());

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner)
        {

            List<Cell> cellList = result.listCells();

            if (cellList != null)
            {


                for (Cell cell : cellList)
                {
                    byte[] family_name = CellUtil.cloneFamily(cell);
                    byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                    byte[] rowkey = CellUtil.cloneRow(cell);
                    byte[] value = CellUtil.cloneValue(cell);
                    //判断id和age字段，这两个字段是整形值
                    if ("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name)))
                    {
                        System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toInt(value));
                    }
                    else
                    {
                        System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toString(value));
                    }
                }

            }
        }
    }

    /**
     * 过滤器  003  RowFilter
     */

    public void filterRowKey() throws IOException
    {
        Scan scan = new Scan();

        BinaryComparator comparator = new BinaryComparator("0003".getBytes());

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, comparator);

        scan.setFilter(rowFilter);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断id和age字段，这两个字段是整形值
                if("age".equals(Bytes.toString(qualifier_name))  || "id".equals(Bytes.toString(qualifier_name))){
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toString(value));
                }
            }
        }

    }

    /**
     * 通过familyFilter来实现列族的过滤
     * 需要过滤，列族名包含f2
     * f1  f2   hello   world
     */
    @Test
    public void familyFilter() throws IOException {
        //Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        SubstringComparator substringComparator = new SubstringComparator("f2");
        //通过familyfilter来设置列族的过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, substringComparator);

        scan.setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断id和age字段，这两个字段是整形值
                if("age".equals(Bytes.toString(qualifier_name))  || "id".equals(Bytes.toString(qualifier_name))){
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toString(value));
                }
            }
        }
    }

    /**
     * 列名过滤器 只查询包含name列的值  QualifierFilter
     */
    @Test
    public void  qualifierFilter() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("name");
        //定义列名过滤器，只查询列名包含name的列
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    private void printResult(ResultScanner scanner) {
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断id和age字段，这两个字段是整形值
                if("age".equals(Bytes.toString(qualifier_name))  || "id".equals(Bytes.toString(qualifier_name))){
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toString(value));
                }
            }
        }
    }
}
