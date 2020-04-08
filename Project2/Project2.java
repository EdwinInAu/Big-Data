//
// Project 2 - Spark
//
// Authors:
//
// Chongshi Wang
//

/*
* Brief Solution:
* read the input txt file
* count the total number of nodes
* change input to the String + Triple format and initialize the distance and parent
* use the modified input to start the Dijkstra algorithm (number of cycles = the total number of nodes)
* process the output of dijkstra algorithm according to the length of final distance (using Sort Class)
* adjust the output format as required (using Result Class)
* finally, save the output as a txt file
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AssigTwoz5190861 {

	/*
	 *The comparision function
	 *From small to large
	 */

	public static class IntegerCompartor implements Comparator<Integer>, Serializable {

		@Override
		public int compare(Integer a1, Integer a2) {
			return a1 - a2;
		}

	}

	/*
	 *Sort -> (String node, ArrayList<String> parent, HashMap<String,Integer> hash)
	 *Sort has three elements
	 *Main purpose of this function:
	 *This function is used to swap the key(String node) and the value._1(Integer distance) of Triple Class
	 *And facilitate the operation (group by key) to sort the final output according to the length of distance
	 *The toString function of Sort:  '(' + node + ',' + parent + ',' + hash + ')'
	 */

	public static class Sort implements Serializable {
		String node;
		ArrayList<String> parent;
		HashMap<String, Integer> hash;

		public String getNode() {
			return node;
		}

		public void setNode(String node) {
			this.node = node;
		}

		public ArrayList<String> getParent() {
			return parent;
		}

		public void setParent(ArrayList<String> parent) {
			this.parent = parent;
		}

		public HashMap<String, Integer> getHash() {
			return hash;
		}

		public void setHash(HashMap<String, Integer> hash) {
			this.hash = hash;
		}

		public Sort(String node, ArrayList<String> parent, HashMap<String, Integer> hash) {
			this.node = node;
			this.parent = parent;
			this.hash = hash;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append('(').append(node).append(',').append(parent).append(',').append(hash).append(')');
			return sb.toString();
		}

	}

	/*
	 *Result -> String node , Integer distance , path1 - path2 - path3
	 *Result has three elements
	 *String node, Integer distance and ArrayList<String> parent
	 *Main purpose of this function:
	 *This function is used to adjust the output format as required
	 *The toString function of Sort:  node + ',' + distance + ( ',' + if parent.size > 0: each element of the parent list + '-' to record the path)
	 */

	public static class Result implements Serializable {
		String node;
		Integer distance;
		ArrayList<String> parent;

		public Result(String node, Integer distance, ArrayList<String> parent) {
			this.node = node;
			this.distance = distance;
			this.parent = parent;
		}

		public String getNode() {
			return node;
		}

		public void setNode(String node) {
			this.node = node;
		}

		public Integer getDistance() {
			return distance;
		}

		public void setDistance(Integer distance) {
			this.distance = distance;
		}

		public ArrayList<String> getParent() {
			return parent;
		}

		public void setParent(ArrayList<String> parent) {
			this.parent = parent;
		}

		@Override
		public String toString() {
			StringBuilder sb1 = new StringBuilder();
			sb1.append(node).append(',').append(distance);
			if (parent.size() > 0) {
				String sb2 = "";
				for (int i = 0; i < parent.size(); i++) {
					sb2 += (parent.get(i)).toString() + "-";
				}
				sb2 = sb2.substring(0, sb2.length() - 1);
				sb1.append(',').append(sb2);
			}
			return sb1.toString();
		}
	}

	/*
	 *Triple -> (Integer distance, ArrayList<String> parent, HashMap<String,Integer> hash)
	 *Triple has three elements
	 *Main purpose of this function:
	 *This function is used to add these three elements in one bracket to facilitate the following operation
	 *The toString function of Sort:  '(' + distance + ',' + parent + ',' + hash + ')'
	 */

	public static class Triple implements Serializable {
		Integer distance;
		ArrayList<String> parent;
		HashMap<String, Integer> hash;

		public Integer getDistance() {
			return distance;
		}

		public void setDistance(Integer distance) {
			this.distance = distance;
		}

		public ArrayList<String> getParent() {
			return parent;
		}

		public void setParent(ArrayList<String> parent) {
			this.parent = parent;
		}

		public HashMap<String, Integer> getHash() {
			return hash;
		}

		public void setHash(HashMap<String, Integer> hash) {
			this.hash = hash;
		}

		public Triple(Integer distance, ArrayList<String> parent, HashMap<String, Integer> hash) {
			this.distance = distance;
			this.parent = parent;
			this.hash = hash;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append('(').append(distance).append(',').append(parent).append(',').append(hash).append(')');
			return sb.toString();
		}
	}

	/*
	 *Dijkstra function
	 *
	 *The first operation:
	 *
	 *For example:
	 * input -> (N0,                ( 0,           [N0],       {N4 = 1 , N 5 = 2})  )
	 *          node      Triple:  distance    parent list		hash(adajacent nodes and distances)
	 * hash function is more easy to do the operation
	 * output -> (N4,   ( 0+1, [N0,N4],    {}  ) ), (N5,   ( 0+2, [N0,N5],    {}  ) ), (N0, ( 0,   [N0], {N4 = 1 , N 5 = 2}))
	 *
	 * The second operation:
	 *
	 * group by key(every node)
	 *
	 * For every node:
	 * select the minimum distance and the parent list that corresponds to it
	 * if hash is not empty, then hash = original hash of the node
	 *
	 * For example:
	 *
	 * input -> (N0, ( 0, [N0], {N4 = 1, N5 = 2 }))?? (N0, ( 1 + 5000 , [N4??N0], {}))?? (N0, ( 2 + 5000 , [N5??N0], {}))
	 *
	 * output ->  0 is the minimum distance and its parent list = [N0]
	 * so output -> (N0 , (0 , [N0], {N4 = 1, N5 = 2}))
	 */

	public static JavaPairRDD<String, Triple> StartDijkstra(JavaPairRDD<String, Triple> inputFile) {
		JavaPairRDD<String, Triple> tmp = inputFile.flatMapToPair((PairFlatMapFunction<Tuple2<String, Triple>, String, Triple>) values -> {

			ArrayList<Tuple2<String, Triple>> output = new ArrayList<Tuple2<String, Triple>>();

			for (Map.Entry value : values._2.getHash().entrySet()) {

				String adjacentNode = (String) value.getKey();
				Integer newDistance = values._2.getDistance() + (Integer) value.getValue();
				ArrayList<String> parent = new ArrayList<String>();
				values._2.getParent().forEach(parent::add);
				parent.add(adjacentNode);
				HashMap<String, Integer> empty = new HashMap<String, Integer>();
				Triple triple = new Triple(newDistance, parent, empty);
				output.add(new Tuple2<String, Triple>(adjacentNode, triple));
			}
			output.add(values);
			return output.iterator();
		});

		JavaPairRDD<String, Iterable<Triple>> processing = tmp.groupByKey();

		JavaPairRDD<String, Triple> finalProcess = processing.mapToPair((PairFunction<Tuple2<String, Iterable<Triple>>, String, Triple>) values -> {
			String node = values._1;
			Integer distance = 5000;
			ArrayList<String> parent1 = new ArrayList<String>();
			HashMap<String, Integer> hash = new HashMap<String, Integer>();

			for (Triple value : values._2) {
				if (value.getDistance() < distance) {
					distance = value.getDistance();
					ArrayList<String> parent2 = new ArrayList<String>();
					value.getParent().forEach(parent2::add);
					parent1 = new ArrayList<String>(parent2);
				}
				if (!value.getHash().isEmpty()) {
					hash = value.getHash();
				}
			}
			Triple triple = new Triple(distance, parent1, hash);
			return new Tuple2<String, Triple>(node, triple);
		});
		return finalProcess;
	}

	/*
	 *Main function
	 *comments below are the explanation of this function
	 */

    public static void main(String[] args) {

        String startPoint = args[0];
        //infinite = 5000
        int inf = 5000;

        SparkConf conf = new SparkConf().setAppName("Assignment2").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        //input reads the input txt file
        JavaRDD<String> input = context.textFile("input.txt");

        // nodes is used to collect every node from the input txt file
        JavaRDD<String> nodes = input.flatMap(contents ->
        {
            ArrayList<String> totalNodes = new ArrayList<String>();
            String[] parts = contents.split(",");
            String node = parts[0];
            String adjacentNode = parts[1];
            totalNodes.add(node);
            totalNodes.add(adjacentNode);
            return totalNodes.iterator();
        });
		//calculate the total number of nodes
        int numberOfNodes = (int) nodes.distinct().count();

        // step1 -> (node, (adjacent node, distance))
		// for example : (N0,(N1,4))
        JavaPairRDD<String, Tuple2<String, Integer>> step1 = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
                String[] parts = line.split(",");
                String node = parts[0];
                String adjacentNode = parts[1];
                Integer distance = Integer.parseInt(parts[2]);
                return new Tuple2<String, Tuple2<String, Integer>>(node, new Tuple2<>(adjacentNode, distance));
            }
        });

        ArrayList<String> node = new ArrayList<String>();
        // group by the same node
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> finalStep1 = step1.groupByKey();
		// step 2: change the tuple2(adjacent node, distance) -> hash (key: adjacent node, value: distance)
		// for example: (N0,{N1 = 1, N2 = 2})
        JavaPairRDD<String, HashMap<String, Integer>> step2 = finalStep1.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, HashMap<String, Integer>>() {

            @Override
            public Tuple2<String, HashMap<String, Integer>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> values)
                    throws Exception {
                HashMap<String, Integer> hash = new HashMap<String, Integer>();
                String node = values._1;
                for (Tuple2<String, Integer> value : values._2) {
                    hash.put(value._1, value._2);
                }
                return new Tuple2<String, HashMap<String, Integer>>(node, hash);
            }
        });
		// if node is starting point then the distance = 0
		// else the distance = 5000
		// add the distance, parent list and the hashmap in the Triple Class
		// output like: (N1, (5000,[N1],{N3 = 3, N4 = 4}))
		//               Node        Triple
        JavaPairRDD<String, Triple> step3 = step2.mapToPair(new PairFunction<Tuple2<String, HashMap<String, Integer>>, String, Triple>() {

            @Override
            public Tuple2<String, Triple> call(Tuple2<String, HashMap<String, Integer>> value) throws Exception {
                String key = value._1;
                Integer distance = inf;
                if (key.compareTo(startPoint) == 0) {
                    distance = 0;
                }
                ArrayList<String> parent = new ArrayList<String>();
                parent.add(key);
                Triple triple = new Triple(distance, parent, value._2);
                return new Tuple2<String, Triple>(key, triple);
            }
        });

        JavaPairRDD<String, Triple> output = step3;
		// start Dijkstra
		// the number of cycles = number of nodes
        for (int i = 0; i < numberOfNodes; i++) {
            output = StartDijkstra(output);
        }

        JavaPairRDD<String, Triple> finalProcess = output;
        // removing the output as required which node is equal to the starting point
        JavaPairRDD<String, Triple> remove = finalProcess.filter(value -> !value._1.equals(startPoint));
		// swap the key(String node) and the value._1(Integer distance) of Triple Class
		// and do the operation (group by key) to sort the output according to the length of distance
        JavaPairRDD<Integer, Sort> sortedOutput = remove.mapToPair(new PairFunction<Tuple2<String, Triple>, Integer, Sort>() {

            @Override
            public Tuple2<Integer, Sort> call(Tuple2<String, Triple> values) throws Exception {
                String node = values._1;
                Integer distance = values._2.getDistance();
                HashMap<String, Integer> hash = new HashMap<String, Integer>();
                hash = values._2.getHash();
                ArrayList<String> parent = new ArrayList<String>();
                parent = values._2.getParent();
                Sort sort = new Sort(node, parent, hash);

                return new Tuple2<Integer, Sort>(distance, sort);
            }

        });

        JavaPairRDD<Integer, Sort> sortedResult = sortedOutput.sortByKey(new IntegerCompartor());

        // using the Result class to change the elements to the output format as required
		// when the distance >= 5000 -> distance = -1
        JavaRDD<String> finalR = sortedResult.map((Function<Tuple2<Integer, Sort>, String>) value -> {
            Integer distance = value._1;
            String newNode = value._2.getNode();
            if (distance >= inf) {
                distance = -1;
            }
            ArrayList<String> parent = new ArrayList<String>();
            parent = value._2.getParent();

            Result result = new Result(newNode, distance, parent);
            return result.toString();

        });

		finalR.coalesce(1, true).saveAsTextFile(args[2]);
	}
}