package algorithm.clustering;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

/**
 * Mesure de la performance du modèle K-means calculé
 */
public class KMeansPredictionMeasure extends ClusteringAlgorithmMeasure {

    private org.apache.spark.mllib.clustering.KMeansModel kmeansClusters;
    private String dataSetFileName;
    private JavaRDD<Vector> testRdd;
    private JavaRDD<Pair<Vector, Integer>> preds;
    private boolean verbose;
    private KMeansPredictionMeasure(JavaSparkContext sc, Integer nbIter) {
        super(sc, nbIter);
    }

    public KMeansPredictionMeasure(JavaSparkContext sc, Integer nbIter, KMeansModel model, String datasetFileName, double n, boolean verbose){
        this(sc, nbIter);
        this.kmeansClusters = model;
        this.dataSetFileName = datasetFileName;
        System.out.println("FILE PATH = " + datasetFileName());
        this.setTextFile(sc.textFile(getDatasetFilePath()));
        JavaRDD<Vector> all = this.getParsedData();
        JavaRDD<Vector> training = jsc.parallelize(all.take((int)(n*all.count())));
        this.testRdd = all.subtract(training);
        this.verbose = verbose;
    }

    @Override
    protected void executeCore() {
        KMeansModel model = this.kmeansClusters;
        preds = this.testRdd.map(point -> new MutablePair<>(point, model.predict(point)));
        this.testRdd.unpersist(false);
    }
    public void printPredictions(){
        for (Pair prediction : this.preds.collect()) {
            System.out.println("Vector " + prediction.getKey().toString() + " => cluster " + prediction.getValue().toString());
        }
    }
    @Override
    protected void printResults() throws Exception {
        System.out.println("Preds: " + this.preds.collect().size());
        if(this.verbose){  printPredictions(); }
    }

    @Override
    protected void persistResults() throws Exception {
        this.kmeansClusters.save(this.jsc.sc(), "tmp/kmeansModel");
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
