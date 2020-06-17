package com.kevin.hbase2hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class HbaseWriteReducer extends TableReducer<Text, Put, ImmutableBytesWritable>
{

    @Override
    protected void reduce(Text key, Iterable<Put> puts, Context context) throws IOException, InterruptedException
    {
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();

        immutableBytesWritable.set(key.toString().getBytes());

        for (Put put : puts)
        {
            context.write(immutableBytesWritable, put);
        }

    }
}
