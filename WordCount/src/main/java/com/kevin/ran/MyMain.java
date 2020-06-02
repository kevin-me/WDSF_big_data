package com.kevin.ran;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MyMain extends Configured implements Tool
{


    static
    {
        try
        {
            System.load("F:\\hadoop-2.6.0-cdh5.14.2\\bin\\hadoop.dll");

            System.setProperty("hadoop.home.dir", "F:\\hadoop-2.6.0-cdh5.14.2");
        }
        catch (UnsatisfiedLinkError e)
        {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception
    {


        Configuration configuration = new Configuration();
        configuration.set("hello", "world");
        //提交run方法之后，得到一个程序的退出状态码
        int run = ToolRunner.run(configuration, new MyMain(), args);
        //根据我们 程序的退出状态码，退出整个进程
        System.exit(run);
    }


    @Override
    public int run(String[] strings) throws Exception
    {
        //获取Job对象，组装我们的八个步骤，每一个步骤都是一个class类

        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf, "mrdemo1");

        job.setJarByClass(MyMain.class);

        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job, new Path("file:///C:\\Users\\Administrator\\Desktop\\第二节\\6、MapReduce直播(1)(3)\\1、数据&代码资料\\1、wordCount_input\\test"));

        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path("file:///C:\\Users\\Administrator\\Desktop\\第二节\\6、MapReduce直播(1)(3)\\1、数据&代码资料\\1、wordCount_input\\out"));

        boolean a = job.waitForCompletion(true);

        return a ? 0 : 1;
    }
}
