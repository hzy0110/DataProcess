package com.hzy.single;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by Hzy on 2016/7/11.
 */
public class UserCF {
    final static int NEIGHBORHOOD_NUM = 2;// 和相邻多少个用户进行关联求相似度
    final static int RECOMMENDER_NUM = 3;// 每个用户推荐产品的数量

    /**
     * @description DataModel负责存储和提供用户、项目、偏好的计算所需要的数据
     *              UserSimiliarity提供了一些基于某种算法的用户相似度度量的方法
     *              UserNeighborhood定义了一个和某指定用户相似的用户集合
     *              Recommender利用所有的组件来为一个用户产生一个推荐结果，另外他也提供了一系列的相关方法
     * @param args
     * @throws IOException
     * @throws TasteException
     */
    public static void main(String[] args) throws IOException, TasteException {
        String file = "E:/item.csv";// 数据文件路径，可以是压缩文件
        DataModel model = new FileDataModel(new File(file));// 加载数据
        UserSimilarity user = new EuclideanDistanceSimilarity(model);// 计算用户相似度，权重值为(0,1]
        NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(
                NEIGHBORHOOD_NUM, user, model);// 寻找相似用户
        Recommender r = new GenericUserBasedRecommender(model, neighbor, user);
        LongPrimitiveIterator iter = model.getUserIDs();

        while (iter.hasNext()) {
            long uid = iter.nextLong();
            List<RecommendedItem> list = r.recommend(uid, RECOMMENDER_NUM);
            System.out.printf("uid:%s", uid);
            for (RecommendedItem ritem : list) {
                System.out.printf("(%s,%f)", ritem.getItemID(),
                        ritem.getValue());

            }
            System.out.println();
        }

        /**
         * 推荐结果评估
         */
        RecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model)
                    throws TasteException {
                UserSimilarity similarity = new PearsonCorrelationSimilarity(
                        model);
                UserNeighborhood neighborhood = new NearestNUserNeighborhood(2,
                        similarity, model);
                return new GenericUserBasedRecommender(model, neighborhood,
                        similarity);
            }
        };

        IRStatistics stats = evaluator.evaluate(recommenderBuilder, null,
                model, null, 2,
                GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);

        System.out.println("查准率: " + stats.getPrecision());//查准率
        System.out.println("召回率: " + stats.getRecall());//召回率

    }
}
