package cw.maxScorePlayer;

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

public class MaxScorePlayer {

    // Mapper Class
    public static class MaxScorePlayerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable points = new IntWritable();
        private Text playerName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length == 2 && !fields[0].equals("PlayerName")) { // Skip the header
                playerName.set(fields[0]);
                points.set(Integer.parseInt(fields[1]));
                context.write(playerName, points);
            }
        }
    }

    // Reducer Class
    public static class MaxScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text mostScoredPlayer = new Text();
        private IntWritable maxScore = new IntWritable(0);

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > maxScore.get()) {
                maxScore.set(sum);
                mostScoredPlayer.set(key);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(mostScoredPlayer, maxScore);
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Scored Player");
        job.setJarByClass(MaxScorePlayer.class);
        job.setMapperClass(MaxScorePlayerMapper.class);
        job.setReducerClass(MaxScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}