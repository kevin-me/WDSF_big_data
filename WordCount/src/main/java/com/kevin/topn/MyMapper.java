package com.kevin.topn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class MyMapper extends Mapper<IntWritable, Text, OrderBean, DoubleWritable>
{

    DoubleWritable dw = new DoubleWritable();

    OrderBean orderBean = new OrderBean();


    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException
    {

        String record = value.toString();

        String[] split = record.split("\t");

        //13764633023     2014-12-01 02:20:42.000 全视目Allseelook 原宿风暴显色美瞳彩色隐形艺术眼镜1片 拍2包邮    33.6    2       18067781305

        if (split.length == 6)
        {
            orderBean.setUserid(split[0]);

            orderBean.setDatetime(DateParse(split[1]));

            orderBean.setTitle(split[2]);

            orderBean.setUnitPrice(Double.parseDouble(split[3]));

            orderBean.setPurchaseNum(Integer.parseInt(split[4]));

            orderBean.setProduceId(split[5]);

            dw.set(Double.parseDouble(split[3]) * Integer.parseInt(split[4]));

            context.write(orderBean, dw);

        }

    }

    public String DateParse(String args)
    {

        LocalDate localDate = LocalDate.parse(args, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

        return localDate.getYear() + "" + localDate.getMonth().getValue();

    }
}
