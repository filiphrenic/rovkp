package hr.fer.tel.rovkp.dz1.zad3;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by fhrenic on 20/03/2017.
 */
public class Test {
    
    public static void main(String[] args) throws IOException, URISyntaxException {
                
        if (args.length != 2) {
            System.err.println("Expecting arguments: <local_file> <hdfs_file>");
            return;
        }
        
        Path localFile = new Path(args[0]);
        Path hdfsFile = new Path(args[1]);
        
        Configuration configuration = new Configuration();
        LocalFileSystem local = LocalFileSystem.getLocal(configuration);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://cloudera2:8020"), configuration);
        
        if (local.isFile(localFile)) {
            System.err.println(args[0] + " is a local file");
        } else if (local.isDirectory(localFile)) {
            System.err.println(args[0] + " is a local directory");
        } else {
            System.err.println(args[0] + " can't be found on local filesystem");
        }
        
        if (hdfs.isFile(hdfsFile)) {
            System.err.println(args[1] + " is a HDFS file");
        } else if (hdfs.isDirectory(hdfsFile)) {
            System.err.println(args[1] + " is a HDFS directory");
        } else {
            System.err.println(args[1] + " can't be found on HDFS filesystem");
        }

    }
    
    
    
}
