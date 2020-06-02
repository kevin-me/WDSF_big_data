package com.kevin.define;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable>
{
    private FileSplit fileSplit;

    private Configuration configuration;

    private BytesWritable byteWritable;

    private boolean flag = false;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        fileSplit=(FileSplit)inputSplit;

        configuration=taskAttemptContext.getConfiguration();

        byteWritable= new BytesWritable();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if(!flag)
        {
            long length = fileSplit.getLength();

            byte[] bytes= new byte[(int)length];

            Path path=fileSplit.getPath();

            FileSystem fileSystem= path.getFileSystem(configuration);

            FSDataInputStream open= fileSystem.open(path);

            IOUtils.readFully(open,bytes,0,(int)length);

            byteWritable.set(bytes,0,(int)length);

            flag = true;

            return true;

        }


        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException
    {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException
    {
        return byteWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return flag?1.0f:0.0f;
    }

    @Override
    public void close() throws IOException
    {

    }
}
