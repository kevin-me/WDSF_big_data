package com.kevin.define;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class MyMain extends Configured implements Tool
{


    @Override
    public int run(String[] strings) throws Exception
    {
        Configuration configuration =super.getConf();

        Job job = Job.getInstance(configuration, "ssss");



        return 0;
    }
}
