import algorithm.SparkAlgorithmMeasure;
import algorithm.clustering.GaussianMixtureClusteringMeasure;
import algorithm.clustering.KMeansClusteringMeasure;
import algorithm.wordcount.WordCount;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

    public static void main(String[] args){

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");                // limitation du niveau de log
        SparkAlgorithmMeasure algo = new KMeansClusteringMeasure(sc);
        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        try {
            System.out.println("Average time : " + algo.execute() + " ms.");
        }
        catch(Exception ex){
            System.err.println(ex.getMessage());
        }

    }

}
