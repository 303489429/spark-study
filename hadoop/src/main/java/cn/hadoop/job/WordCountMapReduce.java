package cn.hadoop.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by wangzhilong on 2017/6/21.
 */
public class WordCountMapReduce extends Configured implements Tool {

    //step 1: Map
    /**
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
     public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1) ;
        private Text word = new Text() ;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
              word.set(itr.nextToken());
              context.write(word,one);
            }
        }
    }

    //step 2: reduce
    static public class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
         private IntWritable result = new IntWritable() ;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0 ;
            for(IntWritable val : values){
                sum += val.get() ;
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    //step 3: driver code
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"WordCount");
        job.setJarByClass(this.getClass());
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setCombinerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, new WordCountMapReduce(), args);
        //int run = new WordCountMapReduce().run(args);
        System.exit(run);
    }
}
