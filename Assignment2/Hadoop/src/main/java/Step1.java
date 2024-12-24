package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 *  Calculate the number single (w1), pairs (w1,w2) and trio (w1,w2,w3) in the corpus.
 *
 * @Input split from a text file
 * @Output: ((w1), <LongWritable>), ((w1,w2), <LongWritable>), ((w1,w2,w3), <LongWritable>)
 */
public class Step1Count {

    //helper classes
    public class TextArrayWritable extends ArrayWritable {
            public TextArrayWritable() {
                super(Text.class);  // Specify the type of elements
            }
        }
    private TextArrayWritable outputArrayWritable = new TextArrayWritable(); // helper classes for mapper

    public static class MapperClass extends Mapper<LongWritable, Text, TextArrayWritable, LongWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text w1 = new Text();
        private Text w2 = new Text();
        private Text w3 = new Text();

        @Override
        public void map(LongWritable key, Text sentence, Context context) throws IOException,  InterruptedException {
        //TODO: continue from here

            // Tokenize the input sentence
            StringTokenizer tokenizer = new StringTokenizer(sentence.toString());

            // Store the words for constructing n-grams
            String[] words = new String[3];
            int count = 0;

            while (tokenizer.hasMoreTokens()) {
                // Shift words to construct n-grams
                words[count % 3] = tokenizer.nextToken();
                count++;

                if (count >= 1) {
                    // Emit uni-gram (w1)
                    w1.set(words[(count - 1) % 3]);
                    Text[] output = new Text[]{w1};
                    context.write(outputArrayWritable.set(output), one);
                }

                if (count >= 2) {
                    // Emit bi-gram (w1, w2)
                    w1.set(words[(count - 2) % 3]);
                    w2.set(words[(count - 1) % 3]);
                    Text[] output = new Text[]{w1, w2};
                    context.write(outputArrayWritable.set(output), one);
                }

                if (count >= 3) {
                    // Emit tri-gram (w1, w2, w3)
                    w1.set(words[(count - 3) % 3]);
                    w2.set(words[(count - 2) % 3]);
                    w3.set(words[(count - 1) % 3]);
                    Text[] output = new Text[]{w1, w2, w3};
                    context.write(outputArrayWritable.set(output), one);
                }
            } //end of while
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(Step1Count.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        For n_grams S3 files.
//        Note: This is English version,and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path(String.format("%s/arbix.txt" , App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/output_word_count" , App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
