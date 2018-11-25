import algorithm.SparkAlgorithmMeasure;
import algorithm.clustering.GaussianMixtureClusteringMeasure;
import conf.SparkAppConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.logging.Logger;

public class MainGaussianMixtureDemo {
    private static Logger logger = Logger.getLogger(MainKMeansDemo.class.getName());
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        Integer nbIterations = 6;
        if(args.length >= 1){
            nbIterations = Integer.parseInt(args[0]);
        }
        boolean is_prod = SparkAppConfig.IS_PROD;
        //Create a SparkContext to initialize
        String master = is_prod ?  "spark://172.31.23.59:7077" : "local[*]";
        SparkConf conf = new SparkConf().setMaster(master).setAppName("SparkTest");
        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");                // limitation du niveau de log
        String file = "household_power_consumption_very_big.txt";
        Double trainingPercentage = 0.5;
        SparkAlgorithmMeasure algo = new GaussianMixtureClusteringMeasure(sc, 1, nbIterations,  file, trainingPercentage);
        try {
            Long executionTimeMs = algo.execute();
            System.out.println("Time for training of " + trainingPercentage*100 +"% of dataset " + file + ": " + executionTimeMs + " ms.");
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            logger.log(java.util.logging.Level.SEVERE, ex.getMessage());
        }
    }
}
