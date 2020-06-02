package com.kevin.topn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<OrderBean, DoubleWritable, Text, DoubleWritable>
{


    Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);
    }

    @Override
    protected void reduce(OrderBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
    {
        int num = 0;

        for (DoubleWritable value : values)
        {

            if (num < 2)
            {

                String keyOut = key.getUserid() + "" + key.getDatetime();

                text.set(keyOut);

                context.write(text, value);

                num++;

            }
            else
            {
                break;
            }

        }
    }
}
