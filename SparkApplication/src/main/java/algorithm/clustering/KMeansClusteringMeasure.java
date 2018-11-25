package algorithm.clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;

/**
 * Évaluation de la librairie de clustering KMeans de Spark
 */
public class KMeansClusteringMeasure extends ClusteringAlgorithmMeasure {
    private Integer nbClusters = 10;
    private Integer nbIterations = 10;

    private org.apache.spark.mllib.clustering.KMeansModel kmeansClusters;
    private String dataSetFileName;
    private RDD<Vector> rdd;
    private KMeansClusteringMeasure(JavaSparkContext sc, Integer nbIter) {
        super(sc, nbIter);
    }


    public KMeansClusteringMeasure(JavaSparkContext sc, Integer nbIter, Integer nbClusters, Integer nbIterations, String datasetFileName, double n){
        this(sc, nbIter);
        this.nbClusters = nbClusters;
        this.nbIterations = nbIterations;
        this.dataSetFileName = datasetFileName;
        System.out.println("FILE PATH = " + datasetFileName());
        this.setTextFile(sc.textFile(getDatasetFilePath()));
        JavaRDD<Vector> parsedData = this.getParsedData();
        this.rdd = jsc.parallelize(parsedData.take((int)(n*parsedData.count()))).rdd();
    }

    @Override
    protected void executeCore() {
        this.kmeansClusters = KMeans.train(this.rdd, this.nbClusters, this.nbIterations);
    }

    @Override
    protected void printResults() throws Exception {
        System.out.println("Cluster centers:");
        for (Vector center: this.kmeansClusters.clusterCenters()) {
            System.out.println(" " + center);
        }
    }

    @Override
    protected void persistResults() throws Exception {
        this.kmeansClusters.save(this.jsc.sc(), "tmp/kmeansModel");
    }

    @Override
    public Double getPrecision() {
        double WSSSE = this.kmeansClusters.computeCost(this.rdd);
        this.rdd.unpersist(false);
        return WSSSE;
    }

    @Override
    public String datasetFileName() {
        return this.dataSetFileName != null ? this.dataSetFileName : super.datasetFileName();
    }

}
