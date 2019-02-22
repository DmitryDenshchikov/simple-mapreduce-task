package home.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class WordCount {

    public static class RecordsMapper
            extends Mapper<Object, Text, IntWritable, Text>{

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] recordsArray = value.toString().split("\r\n");
            for(String record : recordsArray) {
                context.write(new IntWritable(record.hashCode()), new Text(Arrays.toString(record.getBytes())));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            for (Text val : values) {
                stringBuilder.append(val.charAt(0));
            }
            context.write(key, new Text(stringBuilder.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Strange Job");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(RecordsMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}