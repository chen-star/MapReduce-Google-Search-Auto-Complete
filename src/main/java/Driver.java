
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {

    // args: inputDir, outputDir, noGram, threashold, topK

    public static void main(String[] args) throws  ClassNotFoundException, IOException, InterruptedException {

        String inputDir = args[0];
        String outputDir = args[1];
        String noGram = args[2];
        String threashold = args[3];
        String n = args[4];

        // 1st job
        Configuration conf1 = new Configuration();
        conf1.set("textinputformat.record.delimiter", ".");
        conf1.set("noGram", noGram);

        Job job1 = Job.getInstance(conf1);
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(outputDir));

        job1.waitForCompletion(true);

        // 2nd job
        Configuration conf2 = new Configuration();
        conf2.set("threashold", threashold);
        conf2.set("n", n);
        DBConfiguration.configureDB(conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/AutoComplete",
                "root",
                "login");

        Job job2 = Job.getInstance(conf2);
        job2.setJobName("LanguageModel");
        job2.setJarByClass(Driver.class);

        //job2.addArchiveToClassPath(new Path("mysql-connector-java-5.1.44-bin.jar"));

        // when mapper and reducer 的output key value不一样时
        // 需要设置map output
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setMapperClass(LanguageModel.Map.class);
        job2.setReducerClass(LanguageModel.Reduce.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        DBOutputFormat.setOutput(job2, "output", new String[] {"starting_phrase", "following_word", "count"});
        TextInputFormat.setInputPaths(job2, outputDir);
        job2.waitForCompletion(true);

    }

}
