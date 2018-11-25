import algorithm.SparkAlgorithmMeasure;
import algorithm.stats.PearsonCorrelationsMeasure;
import conf.SparkAppConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.logging.Logger;

public class MainCorrelationDemo {
    private static Logger logger = Logger.getLogger(MainKMeansDemo.class.getName());
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        Integer col1 = 2;
        Integer col2 = 3;
        if(args.length >= 1 ){
            col1 = Integer.parseInt(args[0]);
        }
        if(args.length >= 2){
            col2 = Integer.parseInt(args[1]);
        }
        //Create a SparkContext to initialize
        String master = SparkAppConfig.IS_PROD ? "spark://172.31.23.59:7077" : "local[*]";
        SparkConf conf = new SparkConf().setMaster(master).setAppName("SparkTest");
        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");                // limitation du niveau de log
        String file = "household_power_consumption_very_big.txt";

        SparkAlgorithmMeasure algo = new PearsonCorrelationsMeasure(sc, 1, col1, col2, file);
        try {
            Long executionTimeMs = algo.execute();
            System.out.println("Time for 100% of dataset " + file + ": " + executionTimeMs + " ms.");
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            logger.log(java.util.logging.Level.SEVERE, ex.getMessage());
        }
    }
}
