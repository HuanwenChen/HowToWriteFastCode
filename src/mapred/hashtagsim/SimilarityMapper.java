package mapred.hashtagsim;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Map<String, Integer> jobFeatures = null;

    /**
     * We compute the inner product of feature vector of every hashtag with that
     * of #job
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        System.out.println(line);
        String[] hashtag_featureVector = line.split("\\s+", 2);
        String word = hashtag_featureVector[0];
        List<String> hashTag = new ArrayList<String>();
        List<Integer> count = new ArrayList<Integer>();
        String[] TagCounts = hashtag_featureVector[1].split(";");
        Arrays.sort(TagCounts);


        for (String s : TagCounts) {
            String[] temp = s.split(":");
            hashTag.add(temp[0]);
            count.add(Integer.parseInt(temp[1]));
        }

        int similarities = 0;
        StringBuilder tagPair = new StringBuilder();
        IntWritable valueOutput = new IntWritable();
        Text keyOutput = new Text();
        for (int i = 0; i < hashTag.size() - 1; i++) {
            for (int j = i + 1; j < hashTag.size(); j++) {
                tagPair.setLength(0);
                tagPair.append(hashTag.get(i) + "," + hashTag.get(j));
                similarities = count.get(i) * count.get(j);
                keyOutput.set(tagPair.toString());
                valueOutput.set(similarities);
                context.write(keyOutput, valueOutput);
            }
        }

//		String hashtag = hashtag_featureVector[0];
//		Map<String, Integer> features = parseFeatureVector(hashtag_featureVector[1]);
//
//		Integer similarity = computeInnerProduct(jobFeatures, features);
//		context.write(new IntWritable(similarity), new Text("#job\t" + hashtag));
    }
//
//    /**
//     * This function is ran before the mapper actually starts processing the
//     * records, so we can use it to setup the job feature vector.
//     *
//     * Loads the feature vector for hashtag #job into mapper's memory
//     */
////	@Override
////	protected void setup(Context context) {
////		String AllFeatureVector = context.getConfiguration().get(
////				"AllFeatureVector");
////		jobFeatures = parseFeatureVector(AllFeatureVector);
////	}
//
//    /**
//     * De-serialize the feature vector into a map
//     *
//     * @param featureVector The format is "word1:count1;word2:count2;...;wordN:countN;"
//     * @return A HashMap, with key being each word and value being the count.
//     */
//    private Map<String, Integer> parseFeatureVector(String featureVector) {
//        Map<String, Integer> featureMap = new HashMap<String, Integer>();
//        String[] features = featureVector.split(";");
//        for (String feature : features) {
//            String[] word_count = feature.split(":");
//            featureMap.put(word_count[0], Integer.parseInt(word_count[1]));
//        }
//        return featureMap;
//    }
//
//    /**
//     * Computes the dot product of two feature vectors
//     *
//     * @param featureVector1
//     * @param featureVector2
//     * @return
//     */
//    private Integer computeInnerProduct(Map<String, Integer> featureVector1,
//                                        Map<String, Integer> featureVector2) {
//        Integer sum = 0;
//        for (String word : featureVector1.keySet())
//            if (featureVector2.containsKey(word))
//                sum += featureVector1.get(word) * featureVector2.get(word);
//
//        return sum;
//    }
}














