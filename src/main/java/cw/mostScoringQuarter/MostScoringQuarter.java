package cw.mostScoringQuarter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MostScoringQuarter {
    public static class MostScoringQuarterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text teamQuarterKey = new Text();
        private final static IntWritable scoreValue = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] fields = line.split(",");
            if (fields[0].equalsIgnoreCase("HOMESCORE") || fields.length < 29) return;
            String team = fields[8];
            String quarter = fields[5];
            String score = fields[27];
            if (team.isEmpty() || quarter.isEmpty() || score.isEmpty()) return;
            try {
                teamQuarterKey.set(team + "_Q" + quarter);
                scoreValue.set(Integer.parseInt(score));
                context.write(teamQuarterKey, scoreValue);
            } catch (NumberFormatException e) {
                System.err.println("Invalid Score: " + score);
            }
        }
    }

    public static class MostScoringQuarterCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalScore = 0;
            for (IntWritable val : values) {
                totalScore += val.get();
            }
            context.write(key, new IntWritable(totalScore));
        }
    }

    public static class MostScoringQuarterReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final Text currentTeam = new Text();
        private String maxQuarter = null;
        private int maxScore = Integer.MIN_VALUE;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String teamQuarter = key.toString();
            String[] parts = teamQuarter.split("_Q");
            if (parts.length != 2) {
                return;
            }
            String team = parts[0];
            String quarter = "Q" + parts[1];
            int totalScore = 0;
            for (IntWritable val : values) {
                totalScore += val.get();
            }
            if (!team.equals(currentTeam.toString())) {
                if (!currentTeam.toString().isEmpty()) {
                    context.write(currentTeam, new Text(maxQuarter + " " + maxScore));
                }
                currentTeam.set(team);
                maxQuarter = quarter;
                maxScore = totalScore;
            } else {
                if (totalScore > maxScore) {
                    maxQuarter = quarter;
                    maxScore = totalScore;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!currentTeam.toString().isEmpty()) {
                context.write(currentTeam, new Text(maxQuarter + " " + maxScore));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: MostScoringQuarter <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Most Scoring Quarter");
        job.setJarByClass(MostScoringQuarter.class);
        job.setMapperClass(MostScoringQuarterMapper.class);
        job.setCombinerClass(MostScoringQuarterCombiner.class);
        job.setReducerClass(MostScoringQuarterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}