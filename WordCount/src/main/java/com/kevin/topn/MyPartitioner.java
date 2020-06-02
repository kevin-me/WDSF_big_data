package com.kevin.topn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<OrderBean, DoubleWritable>
{
    @Override
    public int getPartition(OrderBean orderBean, DoubleWritable doubleWritable, int numReduceTasks)
    {
        return (orderBean.getUserid().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
