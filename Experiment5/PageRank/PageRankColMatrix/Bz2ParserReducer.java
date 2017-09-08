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


public class Bz2ParserReducer extends Reducer <Text, PageNode, WritableComparable, Writable> {

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

    public void reduce(Text key, Iterable<PageNode> value, Context context) throws IOException, InterruptedException{
        boolean hasAdjList = false;
        context.getCounter(PageRankColMatrix.counter.TotalPages).increment(1);
        this.addToMap(key.toString());
        PageNode node = new PageNode();
        for(PageNode nodeValue: value){
            if(!nodeValue.isSink && nodeValue.adjList.size() > 0){
                node.adjList.addAll(nodeValue.adjList);
                node.isSink = false;
                node.adjList.addAll(nodeValue.adjList);
//				mos.write(PageRankColMatrix.AdjacencyListFileIdentifier, new Text(key.toString()), node, PageRankColMatrix.AdjacencyListFilePathPrefix);
//				return;
            }
        }
        mos.write(PageRankColMatrix.AdjListFileID, new Text(key.toString()), node,
                PageRankColMatrix.AdjListFilePathPrefix);
//		// include node which have no outgoing but only incoming links (dangling nodes)
//		if(!hasAdjList){			
//			mos.write(PageRankColMatrix.DanglingNodeFileIdentifier, NullWritable.get() , new Text(key.toString()), PageRankColMatrix.DanglingPathPrefix);
//		}		
    }



    public void cleanup(Context context) throws IOException, InterruptedException{
        int reduceId = context.getTaskAttemptID().getTaskID().getId();
        mos.write(PageRankColMatrix.StringToNumberMapFileID,new IntWritable(reduceId), this.rowColNumberPageNameMap, PageRankColMatrix.StringToNumberFilePathPrefix);
        mos.write(PageRankColMatrix.NumOffsetFileID, new IntWritable(reduceId), new LongWritable(this.counter), PageRankColMatrix.OffsetFilePathPrefix);
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