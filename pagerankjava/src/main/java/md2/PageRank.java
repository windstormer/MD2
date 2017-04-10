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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

    public static class MatrixMapper
    extends Mapper<Object, Text, Text, Text>{

        public void map(Object key,  Text value, Context context
            ) throws IOException, InterruptedException {


            String[] itr = value.toString().split("\t");

            context.write(new Text(itr[0]),new Text(itr[1]));
            context.write(new Text("Record"),new Text(itr[0]+","+itr[1]));

        }
    }

    public static class FormattedReducer
    extends Reducer<Text,Text,Text,Text>{
        private MultipleOutputs mos;
        private Text K;
        private Text V;
        public void setup(Context context)
        {
            mos = new MultipleOutputs(context);
        }
        public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {
            int count=0;
            if(!key.toString().equals("Record"))
            {
                String[] temp = new String[50000];
                for (Text val : values) {

                    temp[count] = val.toString();
                    count++;
                }
                for(int i=0;i<count;i++)
                {
                    K = new Text(temp[i]+","+key.toString());
                    V = new Text(new Float(1.0f/((float)count)).toString());
                    mos.write("matrix",K,V,"matrix");
                }

            }else
            {
                int MaxNode=0;
                boolean[] check = new boolean[50000];
                for(Text val : values)
                {
                    String[] itr = val.toString().split(",");
                    check[Integer.parseInt(itr[0])]=true;
                    check[Integer.parseInt(itr[1])]=true;
                    if(Integer.parseInt(itr[0])>MaxNode)
                        MaxNode = Integer.parseInt(itr[0]);

                    if(Integer.parseInt(itr[1])>MaxNode)
                        MaxNode = Integer.parseInt(itr[1]);
                    
                }


                int totalnode=0;
                for(int i=0;i<=MaxNode;i++)
                {
                    if(check[i]==true)
                        totalnode++;
                }
                for(int i=0;i<=MaxNode;i++)
                {
                    if(check[i]==true)
                    {
                        K = new Text(String.valueOf(i));
                        V = new Text(new Float(1.0f/((float)totalnode)).toString());
                        mos.write("vector",K,V,"vector");
                    }
                    
                }
            }
            
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
           mos.close(); 
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
    Job job = new Job(conf, "page rank");
    job.setJarByClass(PageRank.class);
    job.setMapperClass(MatrixMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(FormattedReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleOutputs.addNamedOutput(job,"matrix",TextOutputFormat.class,Text.class,Text.class);
    MultipleOutputs.addNamedOutput(job,"vector",TextOutputFormat.class,Text.class,Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}