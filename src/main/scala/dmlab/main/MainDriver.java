package dmlab.main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * @author Hwanjun Song(KAIST), Jae-Gil Lee(KAIST) and Wook-Shin Han(POSTECH)
 * Created on 16/12/01.
 * To find k-medoids using dmlab.main.PAMAE algorithm
 **/
public class MainDriver {

	private static final String METHOD = "DTW";

	public static void main(String[] args) throws IOException
	{
		if(args.length != 6)
		{	
			System.out.println("Usage: dmlab.main.PAMAE <dmlab.main.Input Path> <# of Clusters> <# of Sampled Objects> <# of Sample> <# of Partition> <# of Iteration>");
			System.exit(1);
		}
		
		//argument
		String inputPath = args[0];
		int numOfClusters = Integer.parseInt(args[1]);
		int numOfSampledObjects = Integer.parseInt(args[2]);
		int numOfSamples = Integer.parseInt(args[3]);
		int numOfCores = Integer.parseInt(args[4]);
		int numOfIteration = Integer.parseInt(args[5]);
		
		//set-up spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("dmlab.main.PAMAE").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<FloatPoint> dataSet = PAMAE.readFile(sc, inputPath, numOfCores);
		executePAMAE(dataSet, numOfClusters, numOfSampledObjects, numOfSamples, numOfCores, numOfIteration, sc);
	}

	public static List<FloatPoint> executePAMAE(JavaRDD<FloatPoint> dataSet, int numOfClusters, int numOfSampledObjects, int numOfSamples, int numOfCores, int numOfIteration, JavaSparkContext sc) throws IOException {
		//set-up output path
		FileWriter fw = new FileWriter("PAMAE_OUTPUT.txt");
		BufferedWriter bw = new BufferedWriter(fw);

		//parsing input file and transform to RDD
		System.out.println("Running Phase 1");
		//Phase I (STEP 1 ~ STEP 3) : Parallel Seeding
		List<FloatPoint> bestSeed = PAMAE.PHASE_I(sc, dataSet, numOfClusters, numOfSampledObjects, numOfSamples, numOfCores);
		System.out.println("Finished phase 1");
		//Phase II (STEP 4 ~ STEP 5) : Parallel Refinement
		//iteration
		List<FloatPoint> finalMedoids=null;
		for(int i=0; i<numOfIteration; i++)
        {
            if(METHOD.equals("DTW")){
                finalMedoids = PAMAE.PHASE_II_DTW_Compatible(sc, dataSet, bestSeed, numOfClusters, numOfSampledObjects, numOfSamples, numOfCores);
            } else {
             finalMedoids = PAMAE.PHASE_II(sc, dataSet, bestSeed, numOfClusters, numOfSampledObjects, numOfSamples, numOfCores);
         }

            //set new class label for next iteration
            for(int j=0; j<finalMedoids.size(); j++)
                finalMedoids.get(j).setClassLabel(j);
            bestSeed = finalMedoids;

            double finalError = PAMAE.FinalError(sc, dataSet, finalMedoids);
         bw.write("["+(i+1)+" iter] CLUSTERING ERROR : " + finalError +"\n");
         if(METHOD.equals("DTW")){
             break;
         }
        }

		bw.write("FINAL K MEDOIDS\n");
		for(int i=0; i<finalMedoids.size(); i++)
            bw.write(finalMedoids.get(i).toString()+"\n");

		//unpersist dataset and bestseed
		dataSet.unpersist();
		bw.close();
		sc.stop();
		sc.close();
		return finalMedoids;
	}
}
