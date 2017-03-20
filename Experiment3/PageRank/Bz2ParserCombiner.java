/**
 * Created by vikasjanardhanan on 2/20/17.
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/***
 * Bz2ParserCombiner is used to combine records before being sent to reducer. Suppose same outlink is present from
 * multiple pages, there would be multiple records from that same outlink key. Such records are combined here before
 * being sent to reducer.
 */
public class Bz2ParserCombiner extends Reducer<Text,PageNode,Text,PageNode>{

    /***
     * Emits one PageNode representation of the given pagename. If value contains an entry that has isSink set to false,
     * only that is emitted. Otherwise the node is a sink, and PageNode corresponding with isSink set as true is emitted
     * @param key: pagename
     * @param pageNodes: List of PageNode corresponding to characterisitcs of this pagename.
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
                context.write(key,node);
                return;
            }
        }
        if(isSink){
            context.write(key,node);
        }

    }

}
