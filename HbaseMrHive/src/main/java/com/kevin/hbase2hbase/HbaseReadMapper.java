package com.kevin.hbase2hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

public class HbaseReadMapper extends TableMapper<Text, Put>
{

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException
    {

        byte[] bytes = key.get();

        String rowKey = bytes.toString();

        Text outKey = new Text(rowKey);

        Put put = new Put(rowKey.getBytes());

        List<Cell> cellList = value.listCells();

        for (Cell cell : cellList)
        {
            byte[] family = CellUtil.cloneFamily(cell);

            //列族是f1
            String family_key = family.toString();

            if ("f1".equals(family_key))
            {

                byte[] qualifier = CellUtil.cloneQualifier(cell);

                //列
                String qualifier_key = qualifier.toString();

                if ("name".equals(qualifier_key) || "age".equals(qualifier_key))
                {

                    put.add(cell);
                }

            }

        }
        if (!put.isEmpty())
        {
            context.write(outKey, put);
        }


    }
}
