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

public class hdfs_test {
    private static final String CORE_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/hdfs-site.xml";
    private static final String YARN_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/yarn-site.xml";
    private static final Configuration CONF = new Configuration();
    private static final String LOCAL_DIR = "/users/jason92/projects/hdfs/xs-files/";
    private static final String HDFS_DIR = "/user/jason92/";

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Please provide 5 arguments");
            System.exit(1);
        }

        int localFileStartID = Integer.parseInt(args[0]);
        int localFileEndID = Integer.parseInt(args[1]);
        int hdfsDirStartID = Integer.parseInt(args[2]);
        int hdfsDirEndID = Integer.parseInt(args[3]);
        int numberOfCores = Integer.parseInt(args[4]);

        CONF.addResource(new Path(CORE_SITE_PATH_STR));
        CONF.addResource(new Path(HDFS_SITE_PATH_STR));
        CONF.addResource(new Path(YARN_SITE_PATH_STR));

        System.out.println("Starting HDFS upload process...");
        try {
            uploadFilesToHdfs(localFileStartID, localFileEndID, hdfsDirStartID, hdfsDirEndID, numberOfCores);
            System.out.println("HDFS upload process completed.");
        } catch (Exception e) {
            System.err.println("Error during file upload: " + e.getMessage());
        }
    }

    private static void uploadFilesToHdfs(int localFileStartID, int localFileEndID, int hdfsDirStartID, int hdfsDirEndID, int numberOfCores) {
        List<Path> localFilePaths = generateLocalFilePaths(localFileStartID, localFileEndID);
        List<Path> hdfsDirPaths = generateHdfsDirPaths(hdfsDirStartID, hdfsDirEndID);
        List<Future<String>> futures = new ArrayList<>();

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(numberOfCores);
            
            try (FileSystem fs = FileSystem.get(CONF)) {
                for (Path hdfsDirPath : hdfsDirPaths) {
                    for (Path localFilePath : localFilePaths) {
                        // Path localFilePath = localFilePaths.get(i);
                        Future<String> future = executorService.submit(new Callable<String>() {
                            public String call() throws Exception {
                                fs.copyFromLocalFile(false, true, localFilePath, hdfsDirPath);
                                return "upload file " + localFilePath.getName() + " to " + hdfsDirPath.getName() + " finished";
                            }
                        });
                        futures.add(future);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error initializing FileSystem: " + e.getMessage());
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
        } catch (Exception e) {
            System.err.println("Error during file upload: " + e.getMessage());
        } finally {
            executorService.shutdown();
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
