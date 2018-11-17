package algorithm;

import conf.SparkAppConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Interface commune des mesures d'algorithmes Spark
 */
public abstract class SparkAlgorithmMeasure {
    /** Le RDD du fichier */
    private JavaRDD<String> textFile;
    private Integer nbIter;
    protected  JavaSparkContext jsc;
    public SparkAlgorithmMeasure(JavaSparkContext sc, Integer nbIter){
        this.jsc = sc;
        this.nbIter = nbIter;
        String filePath = getDatasetFilePath();
        this.textFile =  sc.textFile(filePath);
}
    protected void setTextFile(JavaRDD<String> textFile){
        this.textFile = textFile;
    }
    /**
     * Exécution de l'algorithme
     * @return la durée d'exécution moyenne en ms sur NB_ITER itérations
     */
    public Long execute() throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("Start = " + startTime);
        for(int i = 0;i<nbIter;i++) {
            executeCore();
            printResults();
            //  persistResults();
        //    System.out.println(i);
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("Stop = " + stopTime);
        return (stopTime - startTime) / nbIter;
    }

    protected abstract void executeCore();
    /** Affichage des résultats */
    protected abstract void printResults() throws Exception;
    /** Le dataset utilisé par l'algorithme*/
    public abstract String datasetFileName();

    /** NOTE: tout fichier doit être référence dans l'appel à spark-submit */
    protected String getDatasetFilePath(){
        return (SparkAppConfig.IS_PROD ? "./"+datasetFileName() : dataSetFilePath() + datasetFileName());
    }
    protected abstract String dataSetFilePath();

    protected JavaRDD<String> getTextFile() {
        return textFile;
    }

    protected abstract void persistResults() throws Exception;
}
