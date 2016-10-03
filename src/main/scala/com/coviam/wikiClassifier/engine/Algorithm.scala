package com.coviam.wikiClassifier.engine

import org.apache.predictionio.controller.{IPersistentModel, IPersistentModelLoader, P2LAlgorithm, Params}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions.lit
import org.apache.spark.SparkConf

/**
  * Created by bansarishah on 8/20/16.
  */
class Algorithm(val ap:AlgorithmParams) extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult]{

  override def train(sc: SparkContext, pd: PreparedData): Model = {
   val nbModel =  NaiveBayes.train(pd.labeledpoints,lambda = ap.lambda)
   val obs = pd.trainingData.contentAndcategory
   val sqlContext = SQLContext.getOrCreate(sc)
   val phraseDataframe = sqlContext.createDataFrame(obs).toDF("content", "category")
   val categories: Map[String,Int] = phraseDataframe.map(row => row.getAs[String]("category")).collect().zipWithIndex.toMap
   Model(nbModel, categories, sc)
  }

  override def predict(model: Model, query: Query): PredictedResult = {

    //val testQry: Seq[Array[String]] = Seq((Array("sport","cricket")))
    val sqlContext = SQLContext.getOrCreate(model.sc)
    val qryInd = query.topics.zipWithIndex
    val df = sqlContext.createDataFrame(qryInd).toDF("words","id")
    df.show()
    var htf = new HashingTF().setInputCol("words").setOutputCol("feature")
    val hm = htf.transform(df)
    hm.show(false)
    val featureSet = hm.map(x => x.getAs[Vector]("feature"))
    val categories = model.categories.map(_.swap)
    val prediction = model.nbModel.predict(featureSet).first()
    val cat = categories(prediction.toInt)
    val prob = model.nbModel.predictProbabilities(featureSet)
    PredictedResult(cat)
  }
}
case class Model( nbModel: NaiveBayesModel,
                  categories : Map[String,Int],
                     sc: SparkContext
                   ) extends IPersistentModel[AlgorithmParams] with Serializable{
  def save(id: String, params: AlgorithmParams, sc: SparkContext): Boolean = {
    nbModel.save(sc, s"/tmp/${id}/nbmodel")
    sc.parallelize(Seq(categories)).saveAsObjectFile(s"/tmp/${id}/categories")
    true
  }
}

object Model extends IPersistentModelLoader[AlgorithmParams, Model]{
  def apply(id: String, params: AlgorithmParams, sc: Option[SparkContext]) = {
    new Model(
      NaiveBayesModel.load(sc.get,s"/tmp/${id}/nbmodel"),
      sc.get.objectFile[Map[String,Int]](s"/tmp/${id}/categories").first,
      sc.get
    )
  }
}

case class AlgorithmParams(val lambda:Double) extends Params
