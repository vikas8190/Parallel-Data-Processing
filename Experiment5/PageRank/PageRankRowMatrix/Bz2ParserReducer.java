import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class Bz2ParserReducer extends Reducer <Text, PageNodeValue, WritableComparable, Writable> {

    private MultipleOutputs mos;

    private long counter;

    private MapWritable rowColNumberPageNameMap;

    private ArrayList<String> danglingNodes;

    public void setup(Context context){
        mos = new MultipleOutputs(context);
        this.counter = 0;
        this.rowColNumberPageNameMap  = new MapWritable();
        this.danglingNodes = new ArrayList<String>();
    }

    public void reduce(Text key, Iterable<PageNodeValue> value, Context context) throws IOException, InterruptedException{
        boolean hasAdjList = false;
        context.getCounter(PageRankRowMatrix.counter.TotalPages).increment(1);
        this.addToMap(key.toString());
        PageNodeValue node = new PageNodeValue();
        for(PageNodeValue nodeValue: value){
            if(!nodeValue.isSink && nodeValue.adjList.size() > 0){
                node.adjList.addAll(nodeValue.adjList);
                node.isSink = false;
                node.adjList.addAll(nodeValue.adjList);
            }
        }
        mos.write(PageRankRowMatrix.AdjListFileID, new Text(key.toString()), node, PageRankRowMatrix.AdjListFilePathPrefix);
    }



    public void cleanup(Context context) throws IOException, InterruptedException{
        int reduceId = context.getTaskAttemptID().getTaskID().getId();
        mos.write(PageRankRowMatrix.StringToNumberMapFileID,new IntWritable(reduceId), this.rowColNumberPageNameMap, PageRankRowMatrix.StringToNumberFilePathPrefix);
        mos.write(PageRankRowMatrix.NumOffsetFileID, new IntWritable(reduceId), new LongWritable(this.counter), PageRankRowMatrix.OffsetFilePathPrefix);
        mos.close();
    }


    public void addToMap(String name){
        Text pageName = new Text(name);
        if(!this.rowColNumberPageNameMap.containsKey(pageName)){
            LongWritable rowNumber = new LongWritable(this.counter++);
            this.rowColNumberPageNameMap.put(pageName, rowNumber);
        }
    }
}