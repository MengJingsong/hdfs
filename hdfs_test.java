import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.commons.cli.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import java.io.*;

public class hdfs_test {

    private static final int NUMBER_OF_CORES = 30;
    private static final String CORE_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/hdfs-site.xml";
    private static final String YARN_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/yarn-site.xml";
    private static final Configuration CONF = new Configuration();
    private static final String LOCAL_DIR = "/users/jason92/projects/hdfs/xs-files/";
    private static final String HDFS_DIR = "/user/jason92/";

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Please provide 5 arguements");
            System.exit(1);
        }

        int localFileStartID = Integer.parseInt(args[0]);
        int localFileEndID = Integer.parseInt(args[1]);
        int uploadBatchSize = Integer.parseInt(args[2]);
        int hdfsDirStartID = Integer.parseInt(args[3]);
        int hdfsDirEndID = Integer.parseInt(args[4]);


        CONF.addResource(new Path(CORE_SITE_PATH_STR));
        CONF.addResource(new Path(HDFS_SITE_PATH_STR));
        CONF.addResource(new Path(YARN_SITE_PATH_STR));



        System.out.println("Starting HDFS upload process...");
        uploadFilesToHdfs(localFileStartID, localFileEndID, uploadBatchSize, hdfsDirStartID, hdfsDirEndID);
        System.out.println("HDFS upload process completed.");
    }

    private static void uploadFilesToHdfs (int localFileStartID, int localFileEndID, int uploadBatchSize, int hdfsDirStartID, int hdfsDirEndID) {
        List<Path> localFilePaths = generateLocalFilePaths(localFileStartID, localFileEndID);
        List<Path> hdfsDirPaths = generateHdfsDirPaths(hdfsDirStartID, hdfsDirEndID);
        List<Future<String>> futures = new ArrayList<>();

        

        try {
            FileSystem fs = FileSystem.get(CONF);
            ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CORES);
            for (Path hdfsDirPath : hdfsDirPaths) {
                for (int i = 0; i < localFilePaths.size(); i++) {
                    // int realBatchSize = uploadBatchSize;
                    // if (i + realBatchSize > localFilePaths.size()) {
                    //     realBatchSize = localFilePaths.size() - i;
                    // }
                    // Path[] srcs = new Path[realBatchSize];
                    // srcs = localFilePaths.subList(i, i + realBatchSize).toArray(srcs);
                    Path localFilePath = localFilePaths.get(i);
                    Future<String> future = executorService.submit(new Callable<String>() {
                        public String call() throws Exception {
                            fs.copyFromLocalFile(false, true, localFilePath, hdfsDirPath);
                            return "upload file " + localFilePath.getName() + " to " + hdfsDirPath.getName() + " finished";
                        }
                    });
                    futures.add(future);
                }
            }

            // Collect the results of the futures
            for (Future<String> future : futures) {
                try {
                    // Block and wait for the future to complete
                    System.out.println(future.get());
                } catch (Exception e) {
                    System.err.println("Exception occurred during file upload:");
                    e.printStackTrace();
                }
            }

            fs.close();
            executorService.shutdown();

        } catch (Exception e) {
            System.err.println("Error loading Hadoop configuration files: " + e.getMessage());
        }
    }

    private static List<Path> generateLocalFilePaths(int start, int end) {
        List<Path> localFilePaths = new ArrayList<>();
        for (int i = start; i < end; i++) {
            String localPath = LOCAL_DIR + "file-" + i;
            localFilePaths.add(new Path(localPath));
        }
        return localFilePaths;
    }

    private static List<Path> generateHdfsDirPaths(int start, int end) {
        List<Path> hdfsDirPaths = new ArrayList<>();
        for (int i = start; i < end; i++) {
            String hdfsDirPath = HDFS_DIR + "xs-files-dir-" + i + "/";
            hdfsDirPaths.add(new Path(hdfsDirPath));
        }
        return hdfsDirPaths;
    }
}