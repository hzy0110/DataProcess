package com.hzy.recommender.ch05;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.MemoryDiffStorage;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.slopeone.DiffStorage;

/**
 * Created by Hzy on 2016/6/22.
 */
public class LimitedMemoryDiffStorage {
    Recommender buildRecommender(DataModel model) throws TasteException {
        DiffStorage diffStorage = new MemoryDiffStorage(
                model, Weighting.WEIGHTED,100000L);
        return new SlopeOneRecommender(
                model,Weighting.WEIGHTED,Weighting.WEIGHTED,diffStorage);
    }
}
