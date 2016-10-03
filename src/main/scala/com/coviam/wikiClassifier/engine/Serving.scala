package com.coviam.wikiClassifier.engine

import org.apache.predictionio.controller.LServing

/**
  * Created by bansarishah on 8/20/16.
  */
class Serving extends LServing[Query, PredictedResult]{
  override def serve(query: Query, predictions: Seq[PredictedResult]): PredictedResult = {
    println("serving called")
    predictions.head
  }
}

case class PredictedResult(Category: String) extends Serializable{}
