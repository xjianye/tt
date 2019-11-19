package map_reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WCMR extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        Configuration conf=new Configuration();
        int run = ToolRunner.run(conf, new WCMR(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {


         //创建job
        Job job = Job.getInstance(this.getConf(), "wc");
        //设置jar包运行的类
        job.setJarByClass(WCMR.class);


        Path inputPath = new Path("file:///e:\\xie.txt");
        FileInputFormat.setInputPaths(job,inputPath);

         //map
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce
        //reduce
        job.setReducerClass(WCReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //ouotput
        Path outputPath = new Path("file:///e:\\output");
        FileOutputFormat.setOutputPath(job,outputPath);

        return job.waitForCompletion(true) ?0:-1;
    }
}
