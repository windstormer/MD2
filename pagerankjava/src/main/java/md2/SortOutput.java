package md2;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text.Comparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortOutput {

    public static class SplitMapper
    extends Mapper<Object, Text, Text, Text>{

        public void map(Object key,  Text value, Context context
            ) throws IOException, InterruptedException {


            String[] itr = value.toString().split(",");
            for(int i=0;i<itr.length-1;i+=2)
            {
                context.write(new Text(itr[i+1]),new Text(itr[i]));
                //context.write(new Text("unsort"),new Text(itr[i]+","+itr[i+1]));
            }
        }
    }

    public static class OutputReducer
    extends Reducer<Text,Text,Text,Text>{
     public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
            for( Text val : values)
            {
                context.write(key,val);
            
            }
        
    }


}



public static void main(int index) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", "\t");
    Job job = new Job(conf, "sort output");
    job.setJarByClass(SortOutput.class);
    job.setMapperClass(SplitMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(OutputReducer.class);
    job.setSortComparatorClass(Text.Comparator.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path("/user/root/output/out"+String.valueOf(index+1)+"/vector-r-00000"));
    FileOutputFormat.setOutputPath(job, new Path("/user/root/output/sorted"));
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
    job.waitForCompletion(true);

    return ;
}
}