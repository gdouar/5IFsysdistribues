package algorithm.machine_learning;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.rdd.RDD;

/**
 * Mesure de l'entraînement du modèle linéaire
 */
public class LinearRegressionTrainingDataMeasure extends SparkMachineLearningAlgorithmMeasure{

    private LinearRegressionModel regressionModel;
    private String dataSetFileName;
    private JavaRDD<LabeledPoint> dataRDD;
    private RDD<LabeledPoint> trainingRdd;
    private Integer nbItRegression;

    /**
     * Constructeur mesure entraînement régression linéaire
     * @param sc le contexte Spark
     * @param nbIter nombre d'itérations de la boucle principale
     * @param nbIterationsLinearModel nombre d'itérations de l'algorithme d'apprentissage à chaque appel
     * @param datasetFileName le nom du dataset
     * @param trainingPercentage pourcentage du dataset utilisé comme entraînement
     */
    public LinearRegressionTrainingDataMeasure(JavaSparkContext sc, Integer nbIter, Integer nbIterationsLinearModel, String datasetFileName, double trainingPercentage) {
        super(sc, nbIter);
        this.dataSetFileName = datasetFileName;
        this.nbItRegression = nbIterationsLinearModel;
        System.out.println("FILE PATH = " + datasetFileName());
        this.setTextFile(sc.textFile(getDatasetFilePath()));
        RDD test = this.getParsedData().rdd();
        this.dataRDD = this.getParsedData();
        this.trainingRdd = (this.dataRDD).sample(false, trainingPercentage, 15L).rdd();
        this.trainingRdd.cache();
    }

    @Override
    protected void executeCore() {
        this.regressionModel = LinearRegressionWithSGD.train(this.trainingRdd, this.nbItRegression);
    }

    @Override
    protected void printResults() throws Exception {
        System.out.println("Computed model : ");
        Vector model = this.regressionModel.weights();
        System.out.println("Y = " + model.apply(0) + "x + " + model.apply(1));

    }

    public JavaRDD<LabeledPoint> getDataRDD(){
        return this.dataRDD;
    }
    public RDD<LabeledPoint> getTrainingRDD(){
        return this.trainingRdd;
    }

    public LinearRegressionModel getLinearModel(){
        return this.regressionModel;
    }

    @Override
    protected void persistResults() throws Exception {
        this.regressionModel.save(this.jsc.sc(), "tmp/kmeansModel");
    }

    @Override
    public String datasetFileName() {
        return this.dataSetFileName != null ? this.dataSetFileName : super.datasetFileName();
    }
}
