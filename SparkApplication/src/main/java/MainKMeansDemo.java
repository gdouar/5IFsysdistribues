import algorithm.clustering.KMeansModelMeasure;
import algorithm.clustering.KMeansPredictionMeasure;
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
        String file = "household_power_consumption_medium.txt";
        Double training_percentage = 0.5D;
        if(args.length >= 1){
            nbClusters = Integer.parseInt(args[0]);
        }
        if(args.length >= 2){
            nbIterations = Integer.parseInt(args[1]);
        }
        if(args.length >= 3){
            file = args[2];
        }
        if(args.length >= 4 ){
            training_percentage = Double.parseDouble(args[3]);
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
        try {
            KMeansModelMeasure modelMesure = new KMeansModelMeasure(sc, 1, nbClusters, nbIterations, file, training_percentage);
            Long executionTimeMs = modelMesure.execute();
            System.out.println("Time for " + training_percentage*100 + "% of training dataset " + file + " for model construction : " + executionTimeMs + " ms.");
            long startTime = System.currentTimeMillis();
            System.out.println("WSSE = " + modelMesure.getPrecision());
            long stopTime = System.currentTimeMillis();
            System.out.println("Model evaluation time : " + (stopTime - startTime) + " ms.");
            KMeansPredictionMeasure evalMesure = new KMeansPredictionMeasure(sc, 1, modelMesure.getKmeansClusters(),file, 1-training_percentage, false);
            executionTimeMs = evalMesure.execute();
            System.out.println("Time for " + (100 - training_percentage*100) + "% of test dataset " + file + " for prediction : " + executionTimeMs + " ms.");
          //  evalMesure.printPredictions();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            logger.log(java.util.logging.Level.SEVERE, ex.getMessage());
        }
    }
}
