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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatVecMulti {

    public static class MatrixMapper
    extends Mapper<Object, Text, Text, Text>{

        public void map(Object key,  Text value, Context context
            ) throws IOException, InterruptedException {
            String[] itr = value.toString().split(",");

            String temp = itr[0];
            context.write(new Text(itr[0]),new Text("M"+","+itr[1]+","+itr[2]));

        }
    }
    public static class VectorMapper
    extends Mapper<Object, Text, Text, Text>{


        public void map(Object key,  Text value, Context context
            ) throws IOException, InterruptedException {
            String[] itr = value.toString().split(",");
            for(int i=0;i<itr.length-1;i+=3)
            {
                if(itr[i+2].equals("Y"))
                {
                    String s = new String(itr[i+3]);
                    String[] connect = s.split("\\|");
                    for(int j=0;j<connect.length;j++)
                    {
                        // context.write(new Text(itr[i]),new Text(s+":"+connect[j]));
                        context.write(new Text(itr[i]),new Text("R"+","+connect[j]+","+itr[i+1]));
                    }
                    i++;
                }


            }


        }
    }


    public static class MultiReducer
    extends Reducer<Text,Text,Text,Text> {
        private Text word;
        private Text cword;
        private MultipleOutputs mos;

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
        String[] splitVal;


        String[] M = new String[50000];
        String[] R = new String[50000];
        int Mindex=0;
        for (Text val : values) {
            splitVal = val.toString().split(",");
            if(splitVal[0].equals("R"))
            {

                R[Integer.parseInt(splitVal[1])]=splitVal[2];
            }else if(splitVal[0].equals("M"))
            {
                M[Integer.parseInt(splitVal[1])]=splitVal[2];
                if(Mindex < Integer.parseInt(splitVal[1]))
                    Mindex = Integer.parseInt(splitVal[1]);
            }
            // context.write(key,val);
        }
        String s="";
        float sum = 0.0f;
        ArrayList<Integer> connect = new ArrayList<Integer>();
        for(int i=0;i<=Mindex;i++)
        {
          s+=String.valueOf(i)+":"+M[i]+"|"+R[i]+", ";
          if(M[i]!=null)
          {

             sum+=Float.parseFloat(M[i])*Float.parseFloat(R[i]);
             connect.add(new Integer(i));
         }
     }
     String out="";
     out+=Float.toString(sum)+",";
     if(connect.size()>0)
        out+="Y,";
    for(int i=0;i<connect.size();i++)
    {
        out+=connect.get(i).toString();
        if(i!=connect.size()-1)
            out+="|";
    }
    word = new Text(out);

    cword = new Text(s.substring(0,s.length()-1));
    mos.write("check",key,cword,"check");

    mos.write("temp",key, word,"temp");
}
}


public static void main(int index) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    Job job = new Job(conf, "Matrix Vector Multiplication "+String.valueOf(index));
    Path inPath1 = new Path("/user/root/output/out1/matrix-r-00000");
    Path inPath2 = new Path("/user/root/output/out"+String.valueOf(index)+"/vector-r-00000");
    job.setJarByClass(MatVecMulti.class);
    job.setMapperClass(MatrixMapper.class);
    job.setMapperClass(VectorMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(MultiReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job,inPath1,TextInputFormat.class, MatrixMapper.class);
    MultipleInputs.addInputPath(job,inPath2,TextInputFormat.class, VectorMapper.class);
    MultipleOutputs.addNamedOutput(job,"check",TextOutputFormat.class,Text.class,Text.class);
    MultipleOutputs.addNamedOutput(job,"temp",TextOutputFormat.class,Text.class,Text.class);

    FileOutputFormat.setOutputPath(job, new Path("/user/root/output/temp"+String.valueOf(index)));
    job.waitForCompletion(true);
    FixValue.main(index);
    return ;
}
}
