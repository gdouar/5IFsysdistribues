package algorithm.clustering;

import algorithm.SparkAlgorithmMeasure;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vectors$;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Date;

/**
 * Évaluation de la librairie de clustering KMeans de Spark
 */
public class KMeansClustering extends SparkAlgorithmMeasure {
    private Integer nbClusters = 10;
    private Integer nbIterations = 10;
    private org.apache.spark.mllib.clustering.KMeansModel kmeansClusters;


    public KMeansClustering(JavaSparkContext sc) {
        super(sc);
    }
    public KMeansClustering(JavaSparkContext sc, Integer nbClusters) {
        this(sc);
        this.nbClusters = nbClusters;
    }
    public KMeansClustering(JavaSparkContext sc, Integer nbClusters, Integer nbIterations){
        this(sc, nbClusters);
        this.nbIterations = nbIterations;

    }
    @Override
    protected void executeCore() {
        JavaRDD<Vector> parsedData = this.getTextFile().map(s -> {
            String[] sarray = s.split(";");
            double[] values = new double[sarray.length];
            values[0] = new Date(sarray[0]).toInstant().toEpochMilli();
            values[1] = DateTime.parse(sarray[1], DateTimeFormat.forPattern("HH:mm:ss")).getMillisOfDay();
            for(int i=2;i<sarray.length;i++) {
                if(sarray[i].equals("?")){
                    return Vectors.dense(new double[0]);
                }
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        }).filter(vector -> vector.size() > 0);         // gestion des outliers
        this.kmeansClusters = KMeans.train(parsedData.rdd(), this.nbClusters, this.nbIterations);
    }

    @Override
    protected void printResults() throws Exception {
        System.out.println("Cluster centers:");
        for (Vector center: this.kmeansClusters.clusterCenters()) {
            System.out.println(" " + center);
        }
   /*     double cost = this.kmeansClusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost); */
    }

    @Override
    public String datasetFileName() {
        return "src/main/resources/household_power_consumption_VerySmall.txt";
    }

    @Override
    protected void persistResults() throws Exception {
        this.kmeansClusters.save(this.jsc.sc(), "tmp/kmeansModel");
    }
}
