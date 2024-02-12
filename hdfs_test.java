import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.cli.*;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Scanner;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;




public class hdfs_test {
    private static final Configuration CONF = new Configuration();

    public static void main(String[] args) {
        String hadoop_version = args[0];
        String core_site_path = "/users/jason92/hadoop-" + hadoop_version + "/etc/hadoop/core-site.xml";
        String hdfs_site_path = "/users/jason92/hadoop-" + hadoop_version + "/etc/hadoop/hdfs-site.xml";
        String yarn_site_path = "/users/jason92/hadoop-" + hadoop_version + "/etc/hadoop/yarn-site.xml";
        String username = args[1];
        int threadSize = Integer.parseInt(args[2]);
	    int dataSize = Integer.parseInt(args[3]);
        String hdfs_dir = "/user/"+username;

        CONF.addResource(new Path(core_site_path));
        CONF.addResource(new Path(hdfs_site_path));
        CONF.addResource(new Path(yarn_site_path));
        
        
        try {

            FileSystem fs = FileSystem.get(CONF);

            for (int i = 0; i < threadSize; i++) {
                int ii = i;
		        int sz = dataSize;
		        byte[] data = new byte[dataSize];
                try {
                    Path path = new Path(hdfs_dir, "file-" + ii);
                    OutputStream os = fs.create(path, true);
                    Thread closeThread = new Thread(() -> {
                        try {
                            Thread.sleep(500);
                            System.out.println("closeThread waked up");
                            os.close();
                            System.out.println("os closed");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    long start = System.nanoTime();
                    closeThread.start();
                    System.out.println("ready");
                    Thread.sleep(5000);
                    System.out.println("go");
                    os.write(data);

                    long end = System.nanoTime();
                    long ms = (end - start) / 1000000;
                    System.out.format("file %d: write %d bytes to hdfs took %d ms%n", ii, sz, ms);
                    os.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            fs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        
    }
}
