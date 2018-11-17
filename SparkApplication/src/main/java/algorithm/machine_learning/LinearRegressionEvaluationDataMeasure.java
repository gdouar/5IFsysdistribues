package algorithm.machine_learning;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * Mesure du temps CPU de l'évaluation d'un modèle SVM
 */
public class LinearRegressionEvaluationDataMeasure extends SparkMachineLearningAlgorithmMeasure {
    private LinearRegressionTrainingDataMeasure measureModel;
    private RDD<LabeledPoint> testRDD;
    private JavaRDD<Tuple2<Object, Object>> scoreAndLabels;
    /**
     * Constructeur
     * @param sc contexte Spark
     * @param nbIter nombre d'itérations de la boucle principale
     * @param measureModel la mesure de l'entraînement du modèle dont l'évaluation est à faire
     */
    LinearRegressionEvaluationDataMeasure(JavaSparkContext sc, Integer nbIter, LinearRegressionTrainingDataMeasure measureModel) {
        super(sc, nbIter);
        this.measureModel = measureModel;
        this.testRDD = measureModel.getDataRDD().subtract(measureModel.getTrainingRDD().toJavaRDD()).rdd();
    }

    @Override
    protected void executeCore() {
   //    JavaRDD<LabeledPoint> caca = testRDD.map(p -> p. ) TODO
    }

    @Override
    protected void printResults() throws Exception {

    }

    @Override
    protected void persistResults() throws Exception {

    }
}
