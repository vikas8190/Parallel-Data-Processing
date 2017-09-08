import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by vikasjanardhanan on 2/20/17.
 */

/***
 * Representation for characteristics of a page. It has its pagerank value, adjacency list and a flag to indicate if its
 * a sink or not.
 */
public class PageNode implements Writable{
    public double pageRank;
    public ArrayList<String> adjList;
    public boolean isSink;

    public PageNode(){
        this.isSink=true;
        this.pageRank=0.0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeDouble(pageRank);
       dataOutput.writeInt(adjList.size());
        for(String node:adjList){
            dataOutput.writeUTF(node);
        }
        dataOutput.writeBoolean(isSink);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        pageRank=dataInput.readDouble();
        adjList=new ArrayList<String>();
        int arraylen=dataInput.readInt();
        for(int i=0;i<arraylen;i++){
            adjList.add(dataInput.readUTF());
        }
        isSink=dataInput.readBoolean();
    }
}
