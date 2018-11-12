import algorithm.SparkAlgorithmMeasure;
import algorithm.clustering.GaussianMixtureClusteringMeasure;
import algorithm.clustering.KMeansClusteringMeasure;
import algorithm.stats.PearsonCorrelationsMeasure;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class Main {

    public static void main(String[] args){

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");                // limitation du niveau de log
        ArrayList<Long> arrayExec = new ArrayList<>();
      /*  for(int i=1;i<=10;i++){
            SparkAlgorithmMeasure algo = new KMeansClusteringMeasure(sc, 10, 10);
        } */
        try {
           // SparkAlgorithmMeasure algo = new KMeansClusteringMeasure(sc, 10, 10);
            SparkAlgorithmMeasure algo = new PearsonCorrelationsMeasure(sc, 4, 5);
            Long executionTimeMs = algo.execute();
            arrayExec.add(executionTimeMs);
            System.out.println("Average time : " + executionTimeMs+ " ms.");
        }
        catch(Exception ex){
            System.err.println(ex.getMessage());
        }

   /*     try {
            SparkAlgorithmMeasure algo = new KMeansClusteringMeasure(sc, 10, 5);
            Long executionTimeMs = algo.execute();
            arrayExec.add(executionTimeMs);
            System.out.println("Average time : " + executionTimeMs+ " ms.");
        }
        catch(Exception ex){
            System.err.println(ex.getMessage());
        } */
        //System.out.println
    }

}