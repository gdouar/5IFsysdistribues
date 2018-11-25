package algorithm.stats;

import algorithm.SparkAlgorithmMeasure;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.stat.Statistics;

/**
 * Statistiques sur la corrélation de deux variables
 */
public class PearsonCorrelationsMeasure extends SparkAlgorithmMeasure{

    private Double correlation;
    private Integer col1;
    private Integer col2;
    private String dataSetFileName;

    public PearsonCorrelationsMeasure(JavaSparkContext sc, Integer nbIter, Integer col1, Integer col2, String dataSetFileName) {
        super(sc, nbIter);
        this.col1 = col1;
        this.col2 = col2;
        this.dataSetFileName = dataSetFileName;
    }

    //TODO refactor
    protected JavaDoubleRDD getColData(Integer colIdx){
        JavaDoubleRDD parsedData = this.getTextFile().mapToDouble(s -> {
            String[] sarray = s.split(";");
            return sarray[colIdx].equals("?") &&  sarray[colIdx] != null ? -1: Double.parseDouble(sarray[colIdx]);
        }).filter(value -> value != -1);         // gestion des outliers
        return parsedData;
    }

    @Override
    protected void executeCore() {
      JavaDoubleRDD rdd = getColData(this.col1);
      JavaDoubleRDD rdd2 = getColData(this.col2);
      this.correlation = Statistics.corr(rdd.srdd(), rdd2.srdd());
    }

    @Override
    protected void printResults() throws Exception {
        System.out.println("Correlation between " + this.col1 + " and " + this.col2 + " is : " + this.correlation);
    }

    @Override
    public String datasetFileName() {
        return this.dataSetFileName != null ? this.dataSetFileName : "household_power_consumption_VerySmall.txt";
    }

    @Override
    protected String dataSetFilePath() {
        return "src/main/resources/";
    }

    @Override
    protected void persistResults() throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Double getPrecision() {
        return null;
    }
}
