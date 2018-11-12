package algorithm.clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;

/**
 * Évaluation de la librairie de clustering KMeans de Spark
 */
public class KMeansClusteringMeasure extends ClusteringAlgorithmMeasure {
    private Integer nbClusters = 10;
    private Integer nbIterations = 5;
    private org.apache.spark.mllib.clustering.KMeansModel kmeansClusters;


    public KMeansClusteringMeasure(JavaSparkContext sc) {
        super(sc);
    }
    public KMeansClusteringMeasure(JavaSparkContext sc, Integer nbClusters) {
        this(sc);
        this.nbClusters = nbClusters;
    }
    public KMeansClusteringMeasure(JavaSparkContext sc, Integer nbClusters, Integer nbIterations){
        this(sc, nbClusters);
        this.nbIterations = nbIterations;

    }
    @Override
    protected void executeCore() {
        JavaRDD<Vector> parsedData = this.getParsedData();
        this.kmeansClusters = KMeans.train(parsedData.rdd(), this.nbClusters, this.nbIterations);
    }

    @Override
    protected void printResults() throws Exception {
        System.out.println("Cluster centers:");
        for (Vector center: this.kmeansClusters.clusterCenters()) {
            System.out.println(" " + center);
        }
   /*     double cost = this.kmeansClusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost); */
    }

    @Override
    public String datasetFileName() {
        return "src/main/resources/household_power_consumption_VerySmall.txt";
    }

    @Override
    protected void persistResults() throws Exception {
        this.kmeansClusters.save(this.jsc.sc(), "tmp/kmeansModel");
    }
}
