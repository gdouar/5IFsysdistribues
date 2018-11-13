package algorithm.regression;
import algorithm.*;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class LinearRegressionMeasure extends SparkAlgorithmMeasure {

    public LinearRegressionMeasure(JavaSparkContext sc,Integer nbIter) {
        super(sc, nbIter);
    }

    @Override
    protected void executeCore(double n) {

    }

    @Override
    protected void printResults() {

    }

    @Override
    public String datasetFileName() {
        return "adult.data";
    }

    @Override
    protected String dataSetFilePath() {
        return "src/main/resources/";
    }

    @Override
    protected void persistResults() {

    }
}
