package com.kevin.ran;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable,Text,IntWritable>
{

     @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

         int count = 0;//计数

         for (IntWritable data :values){

             count+=data.get();

         }

         IntWritable total = new IntWritable(count);

         context.write(key,total);

    }


}
