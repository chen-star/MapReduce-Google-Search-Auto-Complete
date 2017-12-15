import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        int threashold;

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            threashold = conf.getInt("threashold", 8);
        }


        // input: I love Big Data\t10 (key value上次输入默认的分割是\t)
        // output: I love Big : Data=10

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // line: I love Big Data\t10
            String line = value.toString().trim();

            String[] wordsPlusCount = line.split("\t");
            if (wordsPlusCount.length < 2) {
                return;
            }

            String[] words = wordsPlusCount[0].split("\\s+");
            int count = Integer.parseInt(wordsPlusCount[wordsPlusCount.length-1]);

            if(count < threashold){
                return;
            }

            // output key and value
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length-1; i++) {
                sb.append(words[i]).append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length-1];
            if (!((outputKey == null) || (outputKey.length() < 1))) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }
        }

    }


    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        // top n
        int n;

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // key: I love Big
            // values: <data=10, girl=100, you=200...>
            // desc order
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer> reverseOrder());

            for (Text val : values) {
                String cur_val = val.toString().trim();
                String word = cur_val.split("=")[0].trim();
                int count = Integer.parseInt(cur_val.split("=")[1].trim());
                if(tm.containsKey(count)) {
                    tm.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count, list);
                }
            }

            Iterator<Integer> iter = tm.keySet().iterator();

            for (int j = 0; iter.hasNext() && j < n; j++) {
                int keyCount = iter.next();
                List<String> words = tm.get(keyCount);
                for (String curWord : words) {
                    context.write(new DBOutputWritable(key.toString(), curWord, keyCount), NullWritable.get());
                    j++;
                }
            }


        }


    }
}
