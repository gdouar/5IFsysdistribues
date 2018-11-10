package algorithm.regression;
import algorithm.*;
import org.apache.spark.api.java.JavaSparkContext;

public class LogisticRegression extends SparkAlgorithmMeasure {

    public LogisticRegression(JavaSparkContext sc) {
        super(sc);
    }

    @Override
    protected void executeCore() {

    }

    @Override
    protected void printResults() {

    }

    @Override
    public String datasetFileName() {
        return null;
    }

    @Override
    protected void persistResults() {

    }
}
