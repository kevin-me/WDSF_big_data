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

public class HBaseOperate {

    //创建表
    @Test
    public void createTable() throws IOException {
        //client获得与hbase集群的连接
        Configuration configuration = HBaseConfiguration.create();
        //指定一下zookeeper地址
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);

        //admin管理员有权限创建表
        Admin admin = connection.getAdmin();

        //表的描述对象
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("myuser"));

        //表：列族
        hTableDescriptor.addFamily(new HColumnDescriptor("f1"));
        hTableDescriptor.addFamily(new HColumnDescriptor("f2"));

        //admin开始创建
        admin.createTable(hTableDescriptor);

        admin.close();
        connection.close();
    }

    private Connection connection ;
    private final String TABLE_NAME = "myuser";
    private Table table ;

    @Before
    public void initTable () throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181");
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }

    @Test
    public void putData() throws IOException {
        //获取表
        //Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Put put = new Put("0001".getBytes());//创建put对象，并指定rowkey值
        put.addColumn("f1".getBytes(),"name".getBytes(),"zhangsan".getBytes());
        put.addColumn("f1".getBytes(),"age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f1".getBytes(),"id".getBytes(), Bytes.toBytes(25));
        put.addColumn("f1".getBytes(),"address".getBytes(), Bytes.toBytes("地球人"));
        put.addColumn("f2".getBytes(),"angle".getBytes(), Bytes.toBytes("angle"));
        table.put(put);
        table.close();
    }

    /**
     * 查询rowkey为0003的人
     * get -> Result
     */
    @Test
    public void getData() throws IOException {
        //Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        //通过get对象，指定rowkey
        Get get = new Get(Bytes.toBytes("0001"));

        get.addFamily("f1".getBytes());//限制只查询f1列族下面所有列的值
        //查询f2  列族 phone  这个字段
        get.addColumn("f2".getBytes(),"phone".getBytes());
        //通过get查询，返回一个result对象，所有的字段的数据都是封装在result里面了

        Result result = table.get(get);
        List<Cell> cells = result.listCells();  //获取一条数据所有的cell，所有数据值都是在cell里面 的

        if(cells != null) {
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);//获取列族名
                byte[] column_name = CellUtil.cloneQualifier(cell);//获取列名
                byte[] rowkey = CellUtil.cloneRow(cell);//获取rowkey
                byte[] cell_value = CellUtil.cloneValue(cell);//获取cell值
                //需要判断字段的数据类型，使用对应的转换的方法，才能够获取到值
                if("age".equals(Bytes.toString(column_name))  || "id".equals(Bytes.toString(column_name))){
                    System.out.println(Bytes.toString(family_name));
                    System.out.println(Bytes.toString(column_name));
                    System.out.println(Bytes.toString(rowkey));
                    System.out.println(Bytes.toInt(cell_value));
                }else{
                    System.out.println(Bytes.toString(family_name));
                    System.out.println(Bytes.toString(column_name));
                    System.out.println(Bytes.toString(rowkey));
                    System.out.println(Bytes.toString(cell_value));
                }
            }
            //table.close();
        }

    }

    /**
     * hbase的批量插入数据
     */
    @Test
    public void batchInsert() throws IOException {
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        //f1
        put.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(1));
        put.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(30));
        //f2
        put.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("talk is cheap , show me the code"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<Put>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        table.put(listPut);
    }

    /**
     * 不知道rowkey的具体值，我想查询rowkey范围值是0003  到0006
     * select * from myuser  where age > 30  and id < 8  and name like 'zhangsan'
     *
     */
    @Test
    public void scanData() throws IOException {
        //获取table
        //Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();//没有指定startRow以及stopRow  全表扫描
        //只扫描f1列族
        scan.addFamily("f1".getBytes());
        //扫描 f2列族 phone  这个字段
        scan.addColumn("f2".getBytes(),"phone".getBytes());
        scan.setStartRow("0003".getBytes());
        scan.setStopRow("0007".getBytes());

        //通过getScanner查询获取到了表里面所有的数据，是多条数据
        ResultScanner scanner = table.getScanner(scan);
        //遍历ResultScanner 得到每一条数据，每一条数据都是封装在result对象里面了
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
        //table.close();
    }


    /**
     * 查询所有的rowkey比0003小的所有的数据
     */
    @Test
    public void rowFilter() throws IOException {
        //Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        //获取我们比较对象
        BinaryComparator binaryComparator = new BinaryComparator("0003".getBytes());
        /***
         * rowFilter需要加上两个参数
         * 第一个参数就是我们的比较规则
         * 第二个参数就是我们的比较对象
         */
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);
        //为我们的scan对象设置过滤器

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
     * 列名过滤器 只查询包含name列的值
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

    /**
     * 查询哪些字段值  包含数字8
     */
    @Test
    public void contains8() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("8");
        //列值过滤器，过滤列值当中包含数字8的所有的列
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }


    /**
     * select  *  from  myuser where name  = '刘备'
     * 会返回我们符合条件数据的所有的字段
     *
     * SingleColumnValueExcludeFilter  列值排除过滤器
     *  select  *  from  myuser where name  ！= '刘备'
     */
    @Test
    public void singleColumnValueFilter() throws IOException {
        //查询 f1  列族 name  列  值为刘备的数据
        Scan scan = new Scan();
        //单列值过滤器，过滤  f1 列族  name  列  值为刘备的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }


    /**
     * 查询rowkey前缀以  00开头的所有的数据
     */
    @Test
    public  void  prefixFilter() throws IOException {
        Scan scan = new Scan();
        //过滤rowkey以  00开头的数据
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    /**
     * HBase当中的分页
     */
    @Test
    public void hbasePageFilter() throws IOException {
        int pageNum= 3;
        int pageSize = 2;
        Scan scan = new Scan();
        if(pageNum == 1 ){
            //获取第一页的数据
            scan.setMaxResultSize(pageSize);
            scan.setStartRow("".getBytes());
            //使用分页过滤器来实现数据的分页
            PageFilter filter = new PageFilter(pageSize);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            printResult(scanner);
        }else{
            String  startRow = "";
            //扫描数据的调试 扫描五条数据
            int scanDatas = (pageNum - 1) * pageSize + 1;
            scan.setMaxResultSize(scanDatas);//设置一步往前扫描多少条数据
            PageFilter filter = new PageFilter(scanDatas);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] row = result.getRow();//获取rowkey
                //最后一次startRow的值就是0005
                startRow= Bytes.toString(row);//循环遍历我们多有获取到的数据的rowkey
                //最后一条数据的rowkey就是我们需要的起始的rowkey
            }
            //获取第三页的数据
            scan.setStartRow(startRow.getBytes());
            scan.setMaxResultSize(pageSize);//设置我们扫描多少条数据
            PageFilter filter1 = new PageFilter(pageSize);
            scan.setFilter(filter1);
            ResultScanner scanner1 = table.getScanner(scan);
            printResult(scanner1);
        }
    }



    /**
     * 查询  f1 列族  name  为刘备数据值
     * 并且rowkey 前缀以  00开头数据
     */
    @Test
    public  void filterList() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    /**
     * 删除数据
     */
    @Test
    public  void  deleteData() throws IOException {
        Delete delete = new Delete("0003".getBytes());
        table.delete(delete);
    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable() throws IOException {

        //获取管理员对象，用于表的删除
        Admin admin = connection.getAdmin();
        //删除一张表之前，需要先禁用表
        admin.disableTable(TableName.valueOf("user"));
        admin.deleteTable(TableName.valueOf("user"));
    }
}
