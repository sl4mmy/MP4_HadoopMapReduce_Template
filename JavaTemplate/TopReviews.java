import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class TopReviews extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopReviews.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopReviews(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./preF-output"); // new Path("./tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Review Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(DoubleWritable.class);

        jobA.setMapperClass(ReviewCountMap.class);
        jobA.setReducerClass(ReviewCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopReviews.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Reviews");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopReviewsMap.class);
        jobB.setReducerClass(TopReviewsReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopReviews.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class ReviewCountMap extends Mapper<Object, Text, Text, IntWritable> {
        List<String> stopWords;
        String delimiters;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String stopWordsPath = conf.get("stopwords");
            String delimitersPath = conf.get("delimiters");

            this.stopWords = Arrays.asList(readHDFSFile(stopWordsPath, conf).split("\n"));
            this.delimiters = readHDFSFile(delimitersPath, conf);
        }

        // {
        //   "review_id":"oyaMhzBSwfGgemSGuZCdwQ",
        //   "user_id":"Dd1jQj7S-BFGqRbApFzCFw",
        //   "business_id":"YtSqYv1Q_pOltsVPSx54SA",
        //   "stars":5,
        //   "useful":0,
        //   "funny":0,
        //   "cool":0,
        //   "text":"Tremendous service (Big shout out to Douglas) that complemented the delicious food. Pretty expensive establishment (40-50$ avg for your main course), but its definitely backs that up with an atmosphere that's comparable with any of the top tier restaurants across the country.",
        //   "date":1372072885000
        // }
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Calculate scores and pass along with business_id to the reducer
            final String line = value.toString();

            final JSONObject json = new JSONObject(value);

            final String business_id = json.getString("business_id");
            final int stars = json.getInt("stars") - 3;  // Subtract 3 to scale rating from 1-5 star to -2 - 2

            if (stars == 0) {
                context.write(new Text(business_id), new IntWritable(0));

                return;
            }

            final String review = json.getString("text");

            int weight = 0;
            final StringTokenizer tokenizer = new StringTokenizer(review, delimiters);
            while (tokenizer.hasMoreTokens()) {
                final String nextToken = tokenizer.nextToken().trim().toLowerCase();
                if (!stopWords.contains(nextToken)) {
                    weight++;
                }
            }

            context.write(new Text(business_id), new IntWritable(weight * stars));
        }
    }

    public static class ReviewCountReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }

            final double mean = count > 0 ? sum / count : 0.0;

            context.write(key, new DoubleWritable(mean));
        }
    }

    public static class TopReviewsMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Double, String>> countToReviewMap = new TreeSet<Pair<Double, String>>();
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //Calculate weighted review score for each business ID, keeping the count of map <=N
            final Double mean = Double.parseDouble(value.toString());
            final String business_id = key.toString();

            countToReviewMap.add(new Pair<Double, String>(mean, business_id));
            if (countToReviewMap.size() > 10) {
                countToReviewMap.remove(countToReviewMap.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //output the entries of the map
            for (Pair<Double, String> item : countToReviewMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val); // pass this output to reducer
            }
        }
    }

    public static class TopReviewsReduce extends Reducer<NullWritable, TextArrayWritable, Text, NullWritable> {
        private TreeSet<Pair<Double, String>> countToReviewMap = new TreeSet<Pair<Double, String>>();
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val : values) {
                Writable[] pair = (Writable[]) val.toArray();
                final String business_id = pair[0].toString();
                final Double mean = Double.parseDouble(pair[1].toString());

                countToReviewMap.add(new Pair<Double, String>(mean, business_id));
                if (countToReviewMap.size() > 10) {
                    countToReviewMap.remove(countToReviewMap.first());
                }
            }

            for (Pair<Double, String> item : countToReviewMap) {
                Text business_id = new Text(item.second);
                context.write(business_id, NullWritable.get()); // print as final output
            }
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}