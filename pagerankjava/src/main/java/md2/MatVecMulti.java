package md2;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatVecMulti {

    public static class MatrixMapper
        extends Mapper<Object, Text, Text, Text>{

    public void map(Object key,  Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] itr = value.toString().split(",");

        String temp = itr[0];
        context.write(new Text(itr[0]),new Text(itr[1]+","+itr[2]));
        
    }
}
    public static class VectorMapper
        extends Mapper<Object, Text, Text, Text>{


    public void map(Object key,  Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] itr = value.toString().split(",");
        int size = itr.length;
        for(int i=0;i<itr.length;i++)
        {
            for(int j=0;j<itr.length;j++)
            {
                context.write(new Text(String.valueOf(j)),new Text(String.valueOf(i)+","+itr[i]));
            }
        }
        
    }
}


public static class IntSumReducer
        extends Reducer<Text,Text,Text,Text> {
    private Text word;
    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        String[] splitVal;
        ArrayList<String[]> M = new ArrayList<String[]>();
        ArrayList<String[]> N = new ArrayList<String[]>();
        int sum = 0;
        for (Text val : values) {
            splitVal = val.toString().split(",");
            if(splitVal[0].equals("M"))
                M.add(splitVal);
            else if(splitVal[0].equals("N"))
                N.add(splitVal);
        }
        for(String[] m : M)
        {
            for(String[] n : N)
            {
                if(Integer.parseInt(m[1])==Integer.parseInt(n[1]))
                {
                    sum += (Integer.parseInt(m[2])*Integer.parseInt(n[2]));
                }
            }
        }
        word = new Text(String.valueOf(sum));
        context.write(key, word);
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: wordcount <in> <out>");
        System.exit(2);
    }
        conf.set("mapred.textoutputformat.separator", ",");
    Job job = new Job(conf, "word count");
    job.setJarByClass(MatVecMulti.class);
    job.setMapperClass(MatrixMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
