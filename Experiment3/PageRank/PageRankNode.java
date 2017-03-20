import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by vikasjanardhanan on 2/20/17.
 */
public class PageRankNode implements Writable{
    public String pageName;
    public double pageRank;

    public PageRankNode(){

    }
    public PageRankNode(String pageName,double pageRank){
        this.pageName=pageName;
        this.pageRank=pageRank;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeUTF(pageName);
        dataOutput.writeDouble(pageRank);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        pageName=dataInput.readUTF();
        pageRank=dataInput.readDouble();
    }
}
