/**
 * Created by vikasjanardhanan on 3/30/17.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;

import java.util.HashMap;

public class PageNameToNumberMapper extends Mapper <IntWritable, MapWritable, WritableComparable,Writable>{

    public HashMap<Integer,Long> partitionOffset;
    public MapWritable pageNameToNumber;
    public MapWritable pageNumberToString;
    private MultipleOutputs mouts;

    public void setup(Context context) throws IOException{
        mouts = new MultipleOutputs(context);
        this.pageNameToNumber = new MapWritable();
        this.pageNumberToString = new MapWritable();
        this.partitionOffset = new HashMap<Integer, Long>();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        URI[] paths = context.getCacheFiles();
        FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));
        this.createOffsetsSplit(files, conf);
    }


    public void map(IntWritable key, MapWritable value, Context context){
        long offset = this.partitionOffset.get(key.get());
        for (Writable pageName : value.keySet() ){
            //Text pageNameText = (Text) pageName;
            String pageNameString = pageName.toString();
            //String pageNameString = pageNameText.toString();
            Text pageNameText = new Text(pageNameString);
            LongWritable relativePageNo = (LongWritable) value.get(pageName);
            LongWritable correctPageNo = new LongWritable (offset + relativePageNo.get());
            this.pageNameToNumber.put(pageNameText, correctPageNo);
            this.pageNumberToString.put(correctPageNo, pageNameText);
        }
    }

    public void createOffsetsSplit(FileStatus[] files, Configuration conf) throws IOException{
        Long offset = 0L;
        for (int i = 0; i < files.length; i++) {
            Path currFile = files[i].getPath();
            if (currFile.toString()
                    .contains(PageRankColMatrix.PrePrcoessOutputDir + PageRankColMatrix.OffsetSubDir)) {
                SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(currFile));
                System.out.println("status: " + i + " " + currFile.toString());
                IntWritable key = new IntWritable();
                LongWritable value = new LongWritable();
                while (seqFileReader.next(key, value)) {
                    this.partitionOffset.put(key.get(), offset);
                    offset+= value.get();
                    System.out.println("Offset" + offset);
                }
                seqFileReader.close();
            }
        }
    }

    public void cleanup(Context ctx) throws IOException, InterruptedException{
        mouts.write("dummy", NullWritable.get(), this.pageNameToNumber, PageRankColMatrix.StringToNumberFilePathPrefix);
        mouts.write(PageRankColMatrix.NumberToStringID, NullWritable.get(), this.pageNumberToString,
                PageRankColMatrix.NumberToStringPathPrefix);
        mouts.close();
    }




}
