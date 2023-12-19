import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.cli.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.io.BufferedOutputStream;
import java.io.OutputStream;

public class hdfs_test {
    private static final String CORE_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/hdfs-site.xml";
    private static final String YARN_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/yarn-site.xml";
    private static final Configuration CONF = new Configuration();
    private static final String LOCAL_DIR = "/users/jason92/projects/hdfs/xs-files/";
    private static final String HDFS_DIR = "/user/jason92/";

    public static void main(String[] args) {

        CONF.addResource(new Path(CORE_SITE_PATH_STR));
        CONF.addResource(new Path(HDFS_SITE_PATH_STR));
        CONF.addResource(new Path(YARN_SITE_PATH_STR));

        System.out.println("Starting HDFS upload process...");
        try {
            createNewFilesInHdfs(hdfsFileStartID, hdfsFileEndID, hdfsDirStartID, hdfsDirEndID, numberOfCores);
            System.out.println("HDFS upload process completed.");
        } catch (Exception e) {
            System.err.println("Error during file upload: " + e.getMessage());
        }
    }

    private static void appendFileInHdfs(int threadNum, String fileName) {
            
        try {
            FileSystem fs = FileSystem.get(CONF);
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numberOfCores);

            Path path = new Path(HDFS_DIR + fileName);

            if (!fs.exists(path)) {
                System.out.println("File does not exists");
                return;
            }

            for (int i = 0; i < threadNum; i++) {
                executor.submit(() -> {
                    try {
                        FSDataOutputStream outStream = fs.append(path);
                        outStream.writeBytes(fileName);
                        outStream.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    
                });
            }

            executor.shutdown();

            while (!executor.isTerminated()) {
                Thread.sleep(100);
            }

            fs.close();

        } catch (Exception e) {
            System.err.println("Error during file upload: " + e.getMessage());
        }
    }

}
