import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NGramLibraryBuilder {

    // 前两个参数是input:key-value
    // 后两个是output, 作为reducer的input
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int noGram;

        // only invoked once when initialize Mapper class
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            noGram = conf.getInt("noGram", 5);
        }

        //map method: 拆分单词
        //input: read sentences
        //eg. I love Big Data, n=3
        //    I love Big -> 1
        //    love Big Data -> 1
        //    I love -> 1
        // ........
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            line = line.trim().toLowerCase();

            // replace no alphabetic with whitespace
            line = line.replaceAll("[^a-z]+", " ");

            String words[] = line.split("\\s+"); //split by ' ', '\t', '\n', etc.

            if(words.length < 2) {
                return;
            }

            StringBuilder sb;
            for (int i = 0; i < words.length - 1; i++) {
                sb = new StringBuilder();
                sb.append(words[i]);
                // ignore one gram
                for (int j = 1;  i + j < words.length && j < noGram; j++) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //reduce method
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }
}
