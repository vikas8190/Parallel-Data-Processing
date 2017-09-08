/**
 * Created by vikasjanardhanan on 3/30/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;

public class SparseMatrixMultiplyMapper extends Mapper<LongWritable, ColumnContribWritableList, NullWritable, MapWritable> {

    private long totalPages;
    private int iterNo;
    private MapWritable pageRanks;
    private double danglingContrib;
    private double prevDanglingContrib;
    static double alpha = 0.15;
    private MapWritable pageRankMap;
    double precision;

    public void setup(Context context) throws IOException {
        this.pageRankMap = new MapWritable();
        this.pageRanks = new MapWritable();
        Configuration conf = context.getConfiguration();
        this.totalPages = conf.getLong(PageRankRowMatrix.TOTALPAGES, 1);
        this.iterNo = conf.getInt(PageRankRowMatrix.IterationCount, 0);
        this.prevDanglingContrib = conf.getDouble(PageRankRowMatrix.DanglingContrib, 0);

        FileSystem fs = FileSystem.get(conf);
        URI[] paths = context.getCacheFiles();
        FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));
        if (iterNo > 0) {
            FileStatus[] pageRank = fs.listStatus(new Path(paths[1].getPath()));

            this.formPageRankArray(pageRank, conf);
        }
        setDanglingNodeContrib(files, conf, context);
    }

    public void map(LongWritable key, ColumnContribWritableList value, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        for (Writable val : value.get()) {
            ColumnContrib columnContrib = (ColumnContrib) val;
            sum += getpageRank(columnContrib.colNo) * columnContrib.totalContrib;
        }
        double pageRank = alpha/totalPages + (1-alpha) * (danglingContrib + sum);
        this.precision += pageRank;
        pageRankMap.put(new LongWritable(key.get()), new DoubleWritable(pageRank));

    }


    public void formPageRankArray(FileStatus[] files, Configuration conf) throws IOException {
        for (int i = 0; i < files.length; i++) {
            Path currFile = files[i].getPath();
            if (currFile.toString()
                    .contains(PageRankRowMatrix.PrePrcoessOutputDir + (iterNo - 1) + "/part")) {
                SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(currFile));
                NullWritable key = NullWritable.get();
                MapWritable value = new MapWritable();
                while (seqFileReader.next(key, value)) {
                    pageRanks.putAll(value);
                }
                seqFileReader.close();
            }
        }
    }

    public void setDanglingNodeContrib(FileStatus[] files, Configuration conf, Context context) throws IOException {
        this.danglingContrib = 0.0;

        for (int i = 0; i < files.length; i++) {
            Path curFile = files[i].getPath();
            if (curFile.toString().contains(PageRankRowMatrix.DanglingNodeSubDir)) {
                SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(curFile));
                NullWritable key = NullWritable.get();
                LongWritable value = new LongWritable();
                while (seqFileReader.next(key, value)) {
                    danglingContrib += getpageRank(value.get()) * 1.0 / this.totalPages;
                }
                seqFileReader.close();
            }
        }
    }

    public double getpageRank(long pageNumber) {
        if (this.iterNo == 0) {
            return 1.0 / this.totalPages;
        }

        LongWritable key = new LongWritable(pageNumber);
        if(this.pageRanks.containsKey(key)){
            DoubleWritable pageRank = (DoubleWritable) this.pageRanks.get(key);
            return pageRank.get();
        }
        else{
            return (alpha/totalPages + (1-alpha) * (prevDanglingContrib));
        }
    }

    public void cleanup(Context ctx) throws IOException, InterruptedException{
        int mapId = ctx.getTaskAttemptID().getTaskID().getId();
        if(mapId == 0){
            ctx.getCounter(PageRankRowMatrix.counter.delta).setValue(Double.doubleToLongBits(this.danglingContrib));
        }
        System.out.println(this.precision);
        ctx.write(NullWritable.get(), this.pageRankMap);
    }


}
