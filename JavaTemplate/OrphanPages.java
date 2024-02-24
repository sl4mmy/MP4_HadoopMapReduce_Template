import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

public class OrphanPages extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(OrphanPages.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Orphan Pages");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String delimiters = ":";

            final String line = value.toString();
            final StringTokenizer linksTokenizer = new StringTokenizer(line, delimiters);

            final Integer currentPageId = Integer.parseInt(linksTokenizer.nextToken().trim());
            context.write(new IntWritable(currentPageId), new IntWritable(0));

            final String linkedPageIds = linksTokenizer.nextToken().trim();

            final StringTokenizer pageIdTokenizer = new StringTokenizer(linkedPageIds, " ");
            while (pageIdTokenizer.hasMoreTokens()) {
                final String pageIdToken = linksTokenizer.nextToken().trim();
                if (pageIdToken.isEmpty()) {
                    continue;
                }

                final Integer linkedPageId = Integer.parseInt(pageIdToken);
                if (!currentPageId.equals(linkedPageId)) {
                    context.write(new IntWritable(linkedPageId), new IntWritable(1)); // pass this output to reducer
                }
            }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int incomingLinkCount = 0;
            for (IntWritable value : values) {
                incomingLinkCount += value.get();
            }

            if (incomingLinkCount == 0) {
                context.write(key, NullWritable.get());
            }
        }
    }
}
