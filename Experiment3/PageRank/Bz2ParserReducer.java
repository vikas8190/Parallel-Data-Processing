/**
 * Created by vikasjanardhanan on 2/20/17.
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/***
 * Bz2ParserReducer is responsible for emitting records corresponding to each page in the Bz2File. Records for dangling
 * nodes are also sent out as PageNode with IsSink set as true.
 */
public class Bz2ParserReducer extends Reducer<Text,PageNode,Text,PageNode>{

    /***
     * Emits one PageNode representation of the given pagename. If value contains an entry that has isSink set to false,
     * only that is emitted. Otherwise the node is a sink, and PageNode corresponding with isSink set as true is emitted
     * For each page, the global counter for total pages is also updated.
     * @param key : pagename
     * @param pageNodes: List of PageNode representing the given pagename characteristics
     * @param context: used to emit records
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<PageNode> pageNodes, Context context) throws IOException,InterruptedException{

        boolean isSink=true;
        PageNode node=new PageNode();
        node.adjList=new ArrayList<String>();
        node.isSink=true;
        for(PageNode pageNode:pageNodes){

            if(!pageNode.isSink){
                node.adjList=pageNode.adjList;
                isSink=false;
                node.isSink=false;
                context.getCounter(PageRank.counter.TotalPages).increment(1);
                context.write(key,node);
                return;
            }
        }
        if(isSink){
            context.write(key,node);
            context.getCounter(PageRank.counter.TotalPages).increment(1);
        }

    }

}
