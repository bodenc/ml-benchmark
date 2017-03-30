/**
  * Copyright (C) 2017 TU Berlin DIMA
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *         http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package de.tuberlin.dima.mlbench.spark.kmeans.optimizers

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans => SparkKMeans}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import breeze.linalg.{squaredDistance, DenseVector => BDVector, SparseVector => BSVector, Vector => BVector}



object KMeans {

  // *************************************************************************
  //     UTIL FUNCTIONS
  // *************************************************************************

  def getBreezeInitCentersDataSet(sc: SparkContext, inputPath: String, numPartitions: Int): RDD[(Int, BDVector[Double])] = {
    sc
      .textFile(inputPath, numPartitions)
      .map(s => {
        val line = s.replaceAll("[\\(\\)]", "").split(',')
        val point: BDVector[Double] = BDVector[Double](line.slice(0,line.length - 1).map(_.toDouble))
        val index = line.last.toInt
        (index, point)
      })
  }

  def getBreezeDataSet(sc: SparkContext, inputPath: String, numPartitions: Int): RDD[BDVector[Double]] = {
    sc
      .textFile(inputPath, numPartitions)
      .map(s => {
        val point: BDVector[Double] = BDVector[Double](s.split(",").map(_.toDouble))
        point
      })
  }

  def getBreezePointDataSet(sc: SparkContext, inputPath: String, numPartitions: Int): RDD[BDVector[Double]] =
    getBreezeDataSet(sc, inputPath, numPartitions)

  def getBreezeCentroidDataSet(sc: SparkContext, inputPath: String, numPartitions: Int): RDD[BDVector[Double]] =
    getBreezeDataSet(sc, inputPath, numPartitions)

  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************
  /*
   * train method
   * param
   * @data:
   */

  type WeightedPoint = (BDVector[Double], Long)
  def mergeContribs(x: WeightedPoint, y: WeightedPoint): WeightedPoint = {
    (x._1 + y._1, x._2 + y._2)
  }

  def computeBreezeClustering(data: RDD[BDVector[Double]],
                              centroids: Array[(Int, BDVector[Double])],
                              maxIterations: Int): Array[(Int, BDVector[Double])] = {
    var iterations = 0
    var currentCentroids = centroids

    while(iterations < maxIterations) {

      val bcCentroids = data.context.broadcast(currentCentroids)

      val newCentroids: RDD[(Int, (BDVector[Double], Long))] = data.map (point => {
        var minDistance: Double = Double.MaxValue
        var closestCentroidId: Int = -1
        val centers = bcCentroids.value

        centers.foreach(c => { // c = (idx, centroid)
        val distance = squaredDistance(point, c._2)
          if (distance < minDistance) {
            minDistance = distance
            closestCentroidId = c._1
          }
        })

        (closestCentroidId, (point, 1L))
      }).reduceByKey(mergeContribs)

      currentCentroids = newCentroids
        .map(x => {
          val (center, count) = x._2
          val avgCenter = center / count.toDouble
          (x._1, avgCenter)
        }).collect()

      iterations += 1
    }
    currentCentroids
  }

  def computeBreezeCost(data: RDD[BDVector[Double]], centroids: Array[(Int, BDVector[Double])]): Double = {
    val bcCentroids = data.context.broadcast(centroids)
    data.map(p => {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for ((idx, centroid) <- bcCentroids.value) {
        val distance = squaredDistance(p, centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = idx
        }
      }
      minDistance
    }).sum()
  }
}
