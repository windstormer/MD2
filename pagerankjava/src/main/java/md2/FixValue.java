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

public class FixValue {

    public static class ConnectMapper
    extends Mapper<Object, Text, Text, Text>{

        public void map(Object key,  Text value, Context context
            ) throws IOException, InterruptedException {

            context.write(new Text("Connect"),value);

        }
    }

    public static class FixReducer
    extends Reducer<Text,Text,Text,Text>{
        private MultipleOutputs mos;
        private Text K;
        private Text V;
        private float BETA = 0.8f;
        public void setup(Context context)
        {
            mos = new MultipleOutputs(context);
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
         mos.close(); 
     }

     public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
        int count=0;
        String[] itr;
        float cursum=0.0f;
        ArrayList<String[]> record = new ArrayList<String[]>();
        for( Text val : values)
        {
            count++;
            itr = val.toString().split(",");
            record.add(itr);
            cursum+= (Float.parseFloat(itr[1])*BETA);
        }
        float fix = (1.0f-cursum)/((float)count);
        String out="";
        for(int i=0;i<record.size();i++)
        {
            
            String Ks,Vs;
            Ks = record.get(i)[0];
            Vs = new Float(Float.parseFloat(record.get(i)[1])*BETA+fix).toString();
            if(record.get(i)[2].equals("N"))
                out +=(Ks+","+Vs+","+record.get(i)[2]+",");
            else
                out+=(Ks+","+Vs+","+record.get(i)[2]+","+record.get(i)[3]+",");
            
            // context.write(K,V);
        }
            K = new Text(out.substring(0,out.length()-1));
            V = new Text(" ");
        mos.write("vector",K,V,"vector");

    }

}

public static void main(int index) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    Job job = new Job(conf, "fix value "+ String.valueOf(index));
    job.setJarByClass(FixValue.class);
    job.setMapperClass(ConnectMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(FixReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleOutputs.addNamedOutput(job,"vector",TextOutputFormat.class,Text.class,Text.class);
    FileInputFormat.addInputPath(job, new Path("/user/root/output/temp"+String.valueOf(index)+"/temp-r-00000"));
    FileOutputFormat.setOutputPath(job, new Path("/user/root/output/out"+String.valueOf(index+1)));
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
    job.waitForCompletion(true);

    return ;
}
}
