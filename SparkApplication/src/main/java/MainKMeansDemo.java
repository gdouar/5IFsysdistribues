import algorithm.SparkAlgorithmMeasure;
import algorithm.clustering.KMeansClusteringMeasure;
import conf.SparkAppConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.logging.Logger;

public class MainKMeansDemo {
    private static Logger logger = Logger.getLogger(MainKMeansDemo.class.getName());
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        Integer nbClusters = 50;
        Integer nbIterations = 10;
        if(args.length >= 1){
            nbClusters = Integer.parseInt(args[0]);
        }
        if(args.length >= 2){
            nbIterations = Integer.parseInt(args[1]);
        }
        logger.log(java.util.logging.Level.INFO, "nbClusters = " + nbClusters);
        logger.log(java.util.logging.Level.INFO, "nbIterations (per k-means) = " + nbIterations);
        boolean is_prod = SparkAppConfig.IS_PROD;
        //Create a SparkContext to initialize
        String master = is_prod ? "spark://172.31.23.59:7077" : "local[*]";
        SparkConf conf = new SparkConf().setMaster(master).setAppName("SparkTest");
        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");                // limitation du niveau de log
        String file = "household_power_consumption_medium.txt";

        SparkAlgorithmMeasure algo = new KMeansClusteringMeasure(sc, 1, nbClusters, nbIterations, file, 1);
        try {
            Long executionTimeMs = algo.execute();
            System.out.println("Time for 100% of dataset " + file + ": " + executionTimeMs + " ms.");
            long startTime = System.currentTimeMillis();
            System.out.println("WSSE = " + algo.getPrecision());
            long stopTime = System.currentTimeMillis();
            System.out.println("Model evaluation time : " + (stopTime - startTime) + " ms.");
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            logger.log(java.util.logging.Level.SEVERE, ex.getMessage());
        }
    }
}
