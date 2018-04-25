
#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from time import time

import math
import sys
import os

def createRatingDataSet(sc, path):
    print("Loading Rating Data ")

    cleanedRatingData  = sc.textFile(path)
    headerR = cleanedRatingData.take(1)[0]
    ratingsData = cleanedRatingData.filter(lambda line: line!= headerR)\
        .map(lambda line: line.split(",")).map(lambda tokens : (tokens[0], tokens[1], tokens[2])).cache()

    print(ratingsData.take(4))

    print("Loading Rating Data Completed")

    return ratingsData

def createMovieDataSet(sc, path):
    print("Loading Movie Data ")

    cleanedMovieData = sc.textFile(path)
    headerM = cleanedMovieData.take(1)[0]

    moviesData = cleanedMovieData.filter(lambda line: line != headerM)\
        .map(lambda line: line.split(",")).map(lambda tokens : (tokens[0], tokens[2])).cache()

    print("Loading Movie Data Completed")

    return moviesData

def createTrainingTestingRDD(ratingData):    
    trainingRDD, validationRDD, testRDD = ratingData.randomSplit([6, 2, 2])
    predictValidationRDD = validationRDD.map(lambda x: (x[0], x[1]))
    predictTestRDD = testRDD.map(lambda x: (x[0], x[1]))

    return trainingRDD, validationRDD, testRDD, predictValidationRDD, predictTestRDD


def calculateBestRank(trainingRDD, predictValidationRDD, validationRDD, seed, iterations, regularization_parameter):
    rankList = [4, 8, 12]
    errors = [0, 0, 0]
    err = 0
    min_error = float('inf')
    bestRank = -1

    print ("Loop Starts")
    for r in rankList:
        learningModel = ALS.train(trainingRDD, r, seed=seed, iterations=iterations,lambda_=regularization_parameter)
        predictions = learningModel.predictAll(predictValidationRDD).map(lambda r: ((r[0], r[1]), r[2]))
        ratePrediction = validationRDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    
        error = math.sqrt(ratePrediction.map(lambda r: (r[1][0] - r[1][1])**2).mean())
        errors[err] = error
        err += 1
        print ('Rank %s the RMSE is %s' % (r, error))
        if error < min_error:
            min_error = error
            bestRank = r

    print ('The best model was trained with rank %s' % bestRank)

    predictions.take(3)

    ratePrediction.take(3)

    print ("Loop Ends")

    return bestRank

def predictTestData(trainingRDD, predictTestRDD, testRDD, bestRank, seed, iterations, regularization_parameter):

    model = ALS.train(trainingRDD, bestRank, seed=seed, iterations=iterations, lambda_=regularization_parameter)
    predictions = model.predictAll(predictTestRDD).map(lambda r: ((r[0], r[1]), r[2]))
    ratePrediction = testRDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    error = math.sqrt(ratePrediction.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    
    print ('For testing data the RMSE is %s' % (error))



def createTrainigTestingFullRatingData(fullRatingData, seed, iterations, regularization_parameter, bestRank):

    print (" Full Rating Data Set Evaluation")

    trainingRDD, testRDD = fullRatingData.randomSplit([7, 3])
    actualModel = ALS.train(trainingRDD, bestRank, seed=seed, 
                           iterations=iterations, lambda_=regularization_parameter)

    predictTestRdd = testRDD.map(lambda x: (x[0], x[1]))
    predictions = actualModel.predictAll(predictTestRdd).map(lambda r: ((r[0], r[1]), r[2]))
    ratesPred = testRDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    error = math.sqrt(ratesPred.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    
    print ('RMSE is %s' % (error))

    return trainingRDD, testRDD, predictTestRDD

def loadRatingData(sc, path):   
    cleanedData =  sc.textFile('s3://project-test-n/cleaned/output-rating.csv')
    headerR = cleanedData.take(1)[0]
    fullRatingData = cleanedData.filter(lambda line: line!=headerR)\
                                .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

    return fullRatingData

def loadMovieData(sc, path):                                          
    cleanedData = sc.textFile('s3://project-test-n/raw-data/movie_titles.csv')
    headerM = cleanedData.take(1)[0]
    movieData = cleanedData.filter(lambda line: line!=headerM)\
                                .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()

    movieTitle = movieData.map(lambda x: (int(x[0]),x[1]))

    return movieData, movieTitle
    

def getAverages(idRating):
    n = len(idRating[1])
    return idRating[0], (n, float(sum(x for x in idRating[1]))/n)


def loadRDDForOperations(fullRatingData):
    movieIDRatingsRDD = (fullRatingData.map(lambda x: (x[1], x[2])).groupByKey())
    movieIDAvgRatingsRDD = movieIDRatingsRDD.map(getAverages)
    movieRatingCountsRDD = movieIDAvgRatingsRDD.map(lambda x: (x[0], x[1][0]))

    return movieRatingCountsRDD

def gettestData(path):

    with open(path) as f:
       new_user_ratings = [tuple(map(int, i.split(','))) for i in f]
    print(new_user_ratings)

    return new_user_ratings


def createNewModel(completeRatingData, seed, iterations, regularization_parameter, bestRank):

    t0 = time()
    model = ALS.train(completeRatingData, bestRank, seed=seed, 
                              iterations=iterations, lambda_=regularization_parameter)
    tt = time() - t0

    print ("Training time %s seconds" % round(tt,3))
    return model

def transformAndPredictTopMovies(userRecommendationRDD, movieRatingCountRDD, movieTitle):
    recommendationRatingData = userRecommendationRDD.map(lambda x: (x.product, x.rating))
    ratingtitleCountRDD = \
        recommendationRatingData.join(movieTitle).join(movieRatingCountRDD)
    ratingtitleCountRDD.take(3)

    ratingtitleCountRDD = \
        ratingtitleCountRDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
    
    topMovies = ratingtitleCountRDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])

    print ('Movies Top :\n%s' % '\n'.join(map(str, topMovies)))

if __name__ == '__main__':
    print ('Process of Learning Starts')

    conf = SparkConf().setAppName("NetFlix Prediction").set("spark.executor.memory", "12g").set("spark.driver.memory", "12g")
    sc = SparkContext()
   
    rating_data_path = 's3://project-test-n/rank/output1.csv'
    movie_data_path = 's3://project-test-n/movie_titles.csv'
    complete_data_path = 's3://project-test-n/cleaned/output-rating.csv'

    test_path = '/tmp/netflix-test.txt'

    seed = 5
    iterations = 10
    regularization_parameter = 0.1
    new_user_ID = 0

    ratingData = createRatingDataSet(sc, rating_data_path)
    movieData =  createMovieDataSet(sc, movie_data_path)

    trainingRDD, validationRDD, testingRDD, predictValidationRDD, predictTestRDD = createTrainingTestingRDD(ratingData)

    bestRank = calculateBestRank(trainingRDD, predictValidationRDD, validationRDD, seed, iterations, regularization_parameter)

    predictTestData(trainingRDD, predictTestRDD, testingRDD, bestRank, seed, iterations, regularization_parameter)

    print ("Implementation")

    fullRatingData = loadRatingData(sc, complete_data_path)

    print ("Rating count  %s  in the  dataset" % (fullRatingData.count()))

    comtrainingRDD, comtestRDD, compredictTestRDD = createTrainigTestingFullRatingData(fullRatingData, seed, iterations, regularization_parameter, bestRank)

    completeMovieData, movieTitle = loadMovieData(sc, movie_data_path)

    print ("There are %s movies in the complete dataset" % (completeMovieData.count()))

    testUserRating = gettestData(test_path)
    testUserRatingRDD = sc.parallelize(testUserRating)
    print ('New user ratings: %s' % testUserRatingRDD.take(10))

    completeRatingData = fullRatingData.union(testUserRatingRDD)

    model = createNewModel(completeRatingData, seed, iterations, regularization_parameter, bestRank)

    testUserRatingId = map(lambda x: x[1], testUserRating)

    UnratedMovieId = (completeMovieData.filter(lambda x: x[0] not in testUserRatingId).map(lambda x: (new_user_ID, x[0])))

    userRecommendationRDD = model.predictAll(UnratedMovieId)

    completeMovieTitle = movieData.map(lambda x: (int(x[0]),x[1]))

    movieRatingCountsRDD = loadRDDForOperations(fullRatingData)

    transformAndPredictTopMovies(userRecommendationRDD, movieRatingCountsRDD, movieTitle)

    my_movie = sc.parallelize([(0, 500)]) 
    individualMovieRatingRDD = model.predictAll(UnratedMovieId)
    print(individualMovieRatingRDD.take(2))

    sys.exit(0)






