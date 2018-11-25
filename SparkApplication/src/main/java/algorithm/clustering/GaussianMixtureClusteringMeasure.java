package algorithm.clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;

/**
 * Évaluation de la librairie de clustering KMeans de Spark
 */
public class GaussianMixtureClusteringMeasure extends ClusteringAlgorithmMeasure {
    private Integer nbClusters = 10;
    private String dataSetFileName;
    private RDD rdd;
    private org.apache.spark.mllib.clustering.GaussianMixtureModel gaussianMixtureClusters;

    public GaussianMixtureClusteringMeasure(JavaSparkContext sc, Integer nbIter) {
        super(sc, nbIter);
    }


    public GaussianMixtureClusteringMeasure(JavaSparkContext sc, Integer nbIter, Integer nbClusters) {
        this(sc, nbIter);
        this.nbClusters = nbClusters;
    }
    public GaussianMixtureClusteringMeasure(JavaSparkContext sc, Integer nbIter, Integer nbClusters, String datasetFileName, double n){
        this(sc, nbIter, nbClusters);
        this.dataSetFileName = datasetFileName;
        System.out.println("FILE PATH = " + datasetFileName());
        this.setTextFile(sc.textFile(getDatasetFilePath()));
        JavaRDD<Vector> parsedData = this.getParsedData();
        this.rdd = jsc.parallelize(parsedData.take((int)(n*parsedData.count()))).rdd();
    }

    @Override
    protected void executeCore() {
        JavaRDD<Vector> parsedData = this.getParsedData();
        this.gaussianMixtureClusters = new GaussianMixture().setK(this.nbClusters).run(parsedData.rdd());
    }

    @Override
    protected void printResults() throws Exception {
        System.out.println("Cluster centers:");
        for (int j = 0; j < this.gaussianMixtureClusters.k(); j++) {
            System.out.printf("weight=%f\nmu=%s\nsigma=\n%s\n",
                    this.gaussianMixtureClusters.weights()[j],
                    this.gaussianMixtureClusters.gaussians()[j].mu(), this.gaussianMixtureClusters.gaussians()[j].sigma());
        }
   /*     double cost = this.kmeansClusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost); */
    }


    @Override
    protected void persistResults() throws Exception {
        this.gaussianMixtureClusters.save(this.jsc.sc(), "tmp/gaussianModel");
    }

    @Override
    public Double getPrecision() {
        return null;
    }


    @Override
    public String datasetFileName() {
        return this.dataSetFileName != null ? this.dataSetFileName : super.datasetFileName();
    }
}
