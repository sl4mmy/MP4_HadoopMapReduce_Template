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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Link Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "League Popularity");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(PopularityLeagueMap.class);
        jobB.setReducerClass(PopularityLeagueReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
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

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        List<Integer> leaguePageIds;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            final Configuration conf = context.getConfiguration();

            final String leaguePath = conf.get("league");

            this.leaguePageIds = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"))
                    .stream()
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String delimiters = ":";

            final String line = value.toString();
            final StringTokenizer linksTokenizer = new StringTokenizer(line, delimiters);

            final Integer currentPageId = Integer.parseInt(linksTokenizer.nextToken().trim());

            final String linkedPageIds = linksTokenizer.nextToken().trim();

            final StringTokenizer pageIdTokenizer = new StringTokenizer(linkedPageIds, " ");
            while (pageIdTokenizer.hasMoreTokens()) {
                final String pageIdToken = pageIdTokenizer.nextToken().trim();
                if (pageIdToken.isEmpty()) {
                    continue;
                }

                final Integer linkedPageId = Integer.parseInt(pageIdToken);
                if (!currentPageId.equals(linkedPageId) && leaguePageIds.contains(linkedPageId)) {
                    context.write(new IntWritable(linkedPageId), new IntWritable(1)); // pass this output to reducer
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int incomingLinkCount = 0;
            for (IntWritable value : values) {
                incomingLinkCount += value.get();
            }

            context.write(key, new IntWritable(incomingLinkCount));
        }
    }

    public static class PopularityLeagueMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private TreeSet<Pair<Integer, Integer>> countToPageId = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.parseInt(value.toString());
            Integer pageId = Integer.parseInt(key.toString());

            countToPageId.add(new Pair<Integer, Integer>(count, pageId));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //Cleanup operation starts after all mappers are finished
            final List<Pair<Integer, Integer>> tmp = countToPageId.stream().toList().reversed();
            for (Pair<Integer, Integer> item : tmp) {
                Integer[] strings = {item.second, item.first};
                IntArrayWritable val = new IntArrayWritable(strings);
                context.write(NullWritable.get(), val); // pass this output to reducer
            }
        }
    }

    public static class PopularityLeagueReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        private List<Pair<Integer, Integer>> leagueRankings = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            int lowerRankings = 0;
            int previousCount = -1;
            int currentSkipCount = 0;
            for (IntArrayWritable val : values) {
                Writable[] pair = (Writable[]) val.toArray();
                Integer pageId = Integer.parseInt(pair[0].toString());
                Integer linkCount = Integer.parseInt(pair[1].toString());

                if (previousCount != -1 && linkCount > previousCount) {
                    lowerRankings += 1 + currentSkipCount;

                    currentSkipCount = 0;
                } else if (linkCount == previousCount) {
                    currentSkipCount++;
                }

                leagueRankings.add(new Pair<Integer, Integer>(pageId, lowerRankings));

                previousCount = linkCount;
            }

            leagueRankings.sort(Comparator.comparing(leagueRank -> leagueRank.first));
            leagueRankings.sort(Comparator.reverseOrder());

            for (Pair<Integer, Integer> item : leagueRankings) {
                IntWritable pageId = new IntWritable(item.first);
                IntWritable leagueRank = new IntWritable(item.second);
                context.write(pageId, leagueRank); // print as final output
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
