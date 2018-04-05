package de.hpi.data_change.time_series_similarity.dba.java;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ModifiedDBA implements Serializable{

    private final int NIL = -1;
    private final int DIAGONAL = 0;
    private final int LEFT = 1;
    private final int UP = 2;


    public ModifiedDBA(){

    }

    public double distanceTo(double a, double b) {
        return (a - b) * (a - b);
    }

    /**
     * Online Dtw Barycenter Averaging (DBA), that works with an iterator and returns the new average sequence
     * @param initialCenter average sequence to update
     * @param sequences set of sequences to average
     */
    public double[] DBA(double[] initialCenter, Iterator<double[]> sequences) {
        final ArrayList<Pair<Double,Integer>> onlineBarycenter = new ArrayList();
        for (int i = 0; i < initialCenter.length; i++) {
            onlineBarycenter.add(new Pair<Double,Integer>(0.0,0));
        }
        int nbTuplesAverageSeq, i, j, indiceRes;
        double res = 0.0;
        int centerLength = initialCenter.length;
        int seqLength;

        while (sequences.hasNext()) {
            double[] T = sequences.next();
            seqLength = T.length;
            double[][] costMatrix = new double[centerLength][seqLength];
            int[][] pathMatrix = new int[centerLength][seqLength];
            int[][] optimalPathLength = new int[centerLength][seqLength];
            costMatrix[0][0] = distanceTo(initialCenter[0], T[0]);
            pathMatrix[0][0] = NIL;
            optimalPathLength[0][0] = 0;

            for (i = 1; i < centerLength; i++) {
                costMatrix[i][0] = costMatrix[i - 1][0] + distanceTo(initialCenter[i], T[0]);
                pathMatrix[i][0] = UP;
                optimalPathLength[i][0] = i;
            }
            for (j = 1; j < seqLength; j++) {
                costMatrix[0][j] = costMatrix[0][j - 1] + distanceTo(T[j], initialCenter[0]);
                pathMatrix[0][j] = LEFT;
                optimalPathLength[0][j] = j;
            }

            for (i = 1; i < centerLength; i++) {
                for (j = 1; j < seqLength; j++) {
                    indiceRes = DBA.ArgMin3(costMatrix[i - 1][j - 1], costMatrix[i][j - 1], costMatrix[i - 1][j]);
                    pathMatrix[i][j] = indiceRes;
                    switch (indiceRes) {
                        case DIAGONAL:
                            res = costMatrix[i - 1][j - 1];
                            optimalPathLength[i][j] = optimalPathLength[i - 1][j - 1] + 1;
                            break;
                        case LEFT:
                            res = costMatrix[i][j - 1];
                            optimalPathLength[i][j] = optimalPathLength[i][j - 1] + 1;
                            break;
                        case UP:
                            res = costMatrix[i - 1][j];
                            optimalPathLength[i][j] = optimalPathLength[i - 1][j] + 1;
                            break;
                    }
                    costMatrix[i][j] = res + distanceTo(initialCenter[i], T[j]);
                }
            }

            nbTuplesAverageSeq = optimalPathLength[centerLength - 1][seqLength - 1] + 1;

            i = centerLength - 1;
            j = seqLength - 1;

            for (int t = nbTuplesAverageSeq - 1; t >= 0; t--) {
                addToOnlineCenter(onlineBarycenter,i,T[j]);
                switch (pathMatrix[i][j]) {
                    case DIAGONAL:
                        i = i - 1;
                        j = j - 1;
                        break;
                    case LEFT:
                        j = j - 1;
                        break;
                    case UP:
                        i = i - 1;
                        break;
                }
            }
        }

        double[] newCenter = new double[centerLength];
        for (int t = 0; t < centerLength; t++) {
            Pair<Double, Integer> pair = onlineBarycenter.get(t);
            newCenter[t] = pair.getFirst() / pair.getSecond();
        }
        return newCenter;
    }

    private void addToOnlineCenter(ArrayList<Pair<Double, Integer>> onlineBarycenter, int i, double v) {
        Pair<Double, Integer> pair = onlineBarycenter.get(i);
        pair.setFirst(pair.getFirst() + v);
        pair.setSecond(pair.getSecond()+1);
    }

    public double barycenter(List<Double> tab) {
        if (tab.size() < 1) {
            throw new RuntimeException("empty double tab");
        }
        double sum = 0.0;
        sum = 0.0;
        for (Object o : tab) {
            sum += ((Double) o);
        }
        return sum / tab.size();
    }
}
