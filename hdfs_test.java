import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;

import java.io.*;

public class hdfs_test {

        private static final int NUMBER_OF_CORES = 30;
        private static ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CORES);

        public static void main(String[] args) {
                System.out.println("start");

                List<String> localFilePaths = new ArrayList<>();
                String localDir = "/users/jason92/projects/hdfs/xs-files/";
                for (int i = 3076; i < 1000000; i++) {
                        String localPath = localDir + "file-" + i;
                        localFilePaths.add(localPath);
                }

                String coreSitePathStr = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/core-site.xml";
                String hdfsSitePathStr = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/hdfs-site.xml";
                String yarnSitePathStr = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/yarn-site.xml";

                String hdfsDir = "/user/jason92/";
                List<String> hdfsDirPaths = new ArrayList<>();

                for (int i = 0; i < 15; i++) {
                        String hdfsDirPath = hdfsDir + "xs-files-" + (i * 1000000) + "-" + ((i+1) * 1000000) + "/";
                        hdfsDirPaths.add(hdfsDirPath);
                }


                Configuration conf = new Configuration();

                conf.addResource(new Path(coreSitePathStr));
                conf.addResource(new Path(hdfsSitePathStr));
                conf.addResource(new Path(yarnSitePathStr));

                List<Future<String>> futures = new ArrayList<>();
                //for (String localFilePath : localFilePaths) {
                //        for (String hdfsDirPath : hdfsDirPaths) {

                 //               Future<String> future = executorService.submit(new Callable<String>() {
                 //                       public String call() throws Exception {
                 //                               FileSystem fs = FileSystem.get(conf);
                 //                               Path localPath = new Path(localFilePath);

                  //                              Path hdfsPath = new Path(hdfsDirPath + localPath.getName());
                  //                             if (fs.exists(hdfsPath)) {
                  //                                      fs.close();
                  //                                      return "File " + hdfsPath + " already exists";
                  //                              } else {
                  //                                      fs.copyFromLocalFile(false, true, localPath, hdfsPath);
                  //                                      fs.close();
                  //                                      return "Upload " + localPath + " to " + hdfsPath + " successfully";
                  //                              }
                  //                      }
                  //              });
                  //              futures.add(future);
                  //      }
                //}

                for (String hdfsDirPath : hdfsDirPaths) {
                  Future<String> future = executorService.submit(new Callable<String>() {
                    public String call() throws Exception {
                      FileSystem fs = FileSystem.get(conf);
                      for (String localFilePath : localFilePaths) {
                        Path localPath = new Path(localFilePath);
                        Path hdfsPath = new Path(hdfsDirPath + localPath.getName());
                        if (!fs.exists(hdfsPath)) {
                          fs.copyFromLocalFile(false, 
                }                

                for (Future<String> future : futures) {
                        try {
                                System.out.println(future.get());
                        } catch (Exception e) {
                                System.err.println("Exception occured during file uploaded:");
                                e.printStackTrace();
                        }
                }

                executorService.shutdown();

                System.out.println("end");
        }
}
