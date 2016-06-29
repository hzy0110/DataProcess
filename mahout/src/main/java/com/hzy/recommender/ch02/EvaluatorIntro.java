package com.hzy.recommender.ch02;


import org.apache.mahout.cf.taste.impl.model.file.*;
import org.apache.mahout.cf.taste.impl.neighborhood.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.*;
import java.io.*;
import java.util.*;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.common.RandomUtils;

/**
 *
 * @author wentingtu <wentingtu09 at gmail dot com>
 */
public class EvaluatorIntro
{
    public static void main(String[] args) throws IOException, TasteException
    {
        //=导入org.apache.mahout.common.RandomUtils;=
        //这个是产生唯一的种子使得在划分训练和测试数据的时候具有唯一性=
        RandomUtils.useTestSeed();

        DataModel model = new FileDataModel(new File("e:\\intro.csv"));
        //构建评估器，这里用到的性能度量是每个sum( |预测值 - 真实值| ) / 值的个数
        RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
        //=导入 org.apache.mahout.cf.taste.eval.RecommenderBuilder;=
        //这里要涉及用到了一个定义推荐器构造方法的类：RecommenderBuilder
        RecommenderBuilder builder = new RecommenderBuilder()
        {
            //使用方法是重载buildRecommender函数，函数里是构造推荐器的方法
            @Override
            public Recommender buildRecommender(DataModel model)
                    throws TasteException
            {
                UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
                UserNeighborhood neighborhood =
                        new NearestNUserNeighborhood(2, similarity, model);
                return new GenericUserBasedRecommender(model, neighborhood, similarity);
            }
        };
        //=导入 org.apache.mahout.cf.taste.eval.RecommenderEvaluator;=
        //调用评估器，输入有上面构造的推荐器方法，数据模型，训练/全部 比例，验证数据/数据 比例
        double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);
        //输出评价结果：1.0 证明最后的估计结果是  AverageAbsoluteDifference = 1.0
        System.out.println(score);
    }
}