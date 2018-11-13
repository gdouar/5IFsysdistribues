package algorithm.regression;

import algorithm.SparkAlgorithmMeasure;
import org.apache.spark.api.java.JavaSparkContext;

import javax.swing.*;

public abstract class LearningMeasure extends SparkAlgorithmMeasure{

    public LearningMeasure(JavaSparkContext sc,Integer nbIter) {
        super(sc, nbIter);
    }

    @Override
    protected abstract void executeCore(double n);

    @Override
    protected void printResults() throws Exception {

    }

    @Override
    public String datasetFileName() {
        return null;
    }

    @Override
    protected String dataSetFilePath() {
        return null;
    }

    @Override
    protected void persistResults() throws Exception {

    }
}
