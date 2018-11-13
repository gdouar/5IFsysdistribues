package algorithm.clustering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.linalg.Vector;

/**
 * Évaluation de la librairie de clustering KMeans de Spark
 */
public class GaussianMixtureClusteringMeasure extends ClusteringAlgorithmMeasure {
    private Integer nbClusters = 10;
    private org.apache.spark.mllib.clustering.GaussianMixtureModel gaussianMixtureClusters;

    public GaussianMixtureClusteringMeasure(JavaSparkContext sc, Integer nbIter) {
        super(sc, nbIter);
    }


    public GaussianMixtureClusteringMeasure(JavaSparkContext sc, Integer nbIter, Integer nbClusters) {
        this(sc, nbIter);
        this.nbClusters = nbClusters;
    }

    @Override
    protected void executeCore(double n) {
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
}
