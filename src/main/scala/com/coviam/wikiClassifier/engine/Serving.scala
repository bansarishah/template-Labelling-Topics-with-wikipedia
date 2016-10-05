package com.coviam.wikiClassifier.engine

import org.apache.predictionio.controller.LServing

class Serving extends LServing[Query, PredictedResult]{
  override def serve(query: Query, predictions: Seq[PredictedResult]): PredictedResult = {
    predictions.head
  }
}

case class PredictedResult(Category: String) extends Serializable{}
