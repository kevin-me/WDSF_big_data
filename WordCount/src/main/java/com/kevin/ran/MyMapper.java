package com.kevin.ran;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class  MyMapper  extends Mapper<LongWritable, Text,Text, IntWritable>
{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String wordLine= value.toString();

        String words[] = wordLine.split(",");

        Text text = new Text();

        IntWritable count = new IntWritable(1);

        for (String word : words)
        {
            text.set(word);

            context.write(text,count);

        }
    }


}
