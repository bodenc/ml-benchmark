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
package de.tuberlin.dima.mlbench.flink.kmeans.optimizers

import breeze.linalg.functions.euclideanDistance
import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector, Vector}
import breeze.linalg.{squaredDistance, axpy}
import breeze.linalg.{Vector => BVector, DenseVector => BDVector, SparseVector => BSVector}
import scala.collection.JavaConverters._

object KMeans {


  /**
    * Carry out a fixed number of iterations of kmeans clustering using FLink Vectors and BLAS
    *
    * @param points
    * @param centroids
    * @param iterations
    * @return finalCentroids
    */

  def computeFlinkClustering(points: DataSet[Vector], centroids: DataSet[(Int, Vector)], iterations: Int): DataSet[(Int, Vector)] = {
    val finalCentroids: DataSet[(Int, Vector)] = centroids.iterate(iterations) { currentCentroids =>
      val newCentroids = points
        .map(new flinkSelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .map { x => (x._1, x._2, 1L) }.withForwardedFields("_1; _2")
        .groupBy(0)
        .reduce((p1, p2) => {
          val sum = p2._2
          BLAS.axpy(1, p1._2, sum)
          (p1._1, sum, p1._3 + p2._3)
        }).withForwardedFields("_1")
        .map( x => {
          val toDiv = x._2
          BLAS.scal(1.0/x._3.toDouble, toDiv)
          (x._1, toDiv)
        }).withForwardedFields("_1")
      newCentroids
    }

    finalCentroids
  }


  def computeBreezeClustering(points: DataSet[BDVector[Double]], centroids: DataSet[(Int, BDVector[Double])], iterations: Int): DataSet[(Int, BDVector[Double])] = {
    val finalCentroids: DataSet[(Int, BDVector[Double])] = centroids.iterate(iterations) { currentCentroids =>
      val newCentroids = points
        .map(new breezeSelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .groupBy(0)
        .reduce((p1, p2) => {
          (p1._1, p1._2 + p2._2, p1._3 + p2._3)}).withForwardedFields("_1")

      val avgNewCentroids = newCentroids
        .map(x => {
          val avgCenter = x._2 / x._3.toDouble
          (x._1, avgCenter)
        }).withForwardedFields("_1")

      avgNewCentroids
    }

    finalCentroids
  }





  /**
    * determine closest cluster center using Breeze vectors
    */

  @ForwardedFields(Array("*->_2"))
  final class breezeSelectNearestCenter extends RichMapFunction[BDVector[Double], (Int, BDVector[Double], Long)] {
    private var centroids: Traversable[(Int, BDVector[Double])] = null

    /** reads centroids and indexing values from the broadcasted set **/
    override def open(parameters: Configuration): Unit = {
      centroids = getRuntimeContext.getBroadcastVariable[(Int, BDVector[Double])]("centroids").asScala
    }

    override def map(point: BDVector[Double]): (Int, BDVector[Double], Long) = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for ((idx, centroid) <- centroids) {
        val distance = squaredDistance(point, centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = idx
        }
      }
      (closestCentroidId, point, 1L)
    }
  }



  /**
    * determine closest cluster center using flink vectors
    */
  @ForwardedFields(Array("*->_2"))
  final class flinkSelectNearestCenter extends RichMapFunction[Vector, (Int, Vector)] {
    private var centroids: Traversable[(Int,Vector)] = null

    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[(Int,Vector)]("centroids").asScala
    }

    def map(p: Vector): (Int, Vector) = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for ((idx, centroid) <- centroids) {
        val distance = euclideanDistance(p, centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = idx
        }
      }
      (closestCentroidId, p)
    }
  }




  def flinkComputeCost(data: DataSet[Vector], centroids: DataSet[(Int, Vector)]): Double = {
    data.map(new SelectNearestDistance).withBroadcastSet(centroids, "centroids").reduce(_ + _).collect().head
  }

  final class SelectNearestDistance extends RichMapFunction[Vector, Double] {
    private var centroids: Traversable[(Int,Vector)] = null

    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[(Int,Vector)]("centroids").asScala
    }

    def map(p: Vector): Double = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for ((idx, centroid) <- centroids) {

        val distance = euclideanDistance(p, centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = idx
        }
      }
      minDistance
    }
  }



  def breezeComputeCost(data: DataSet[BDVector[Double]], centroids: DataSet[(Int, BDVector[Double])]): Double = {
    data.map(new SelectNearestBreezeDistance).withBroadcastSet(centroids, "centroids").reduce(_ + _).collect().head
  }

  final class SelectNearestBreezeDistance extends RichMapFunction[BDVector[Double], Double] {
    private var centroids: Traversable[(Int, BDVector[Double])] = null

    /** Reads the centroid values from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[(Int,BDVector[Double])]("centroids").asScala
    }

    def map(p: BDVector[Double]): Double = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for ((idx, centroid) <- centroids) {

        val distance = squaredDistance(p, centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = idx
        }
      }
      minDistance
    }
  }





  // *************************************************************************
  //     LOADING DATA FUNCTIONS
  // *************************************************************************

  def getInitCentersDataSet(env: ExecutionEnvironment, inputPath: String): DataSet[(Int, Vector)] = {
    env
      .readTextFile(inputPath)
      .map(s => {
        val line = s.replaceAll("[\\(\\)]", "").split(',')
        val point: DenseVector = DenseVector(line.slice(0,line.length - 1).map(_.toDouble))
        val index = line.last.toInt
        (index, point)
      })
  }

  def getBreezeInitCentersDataSet(env: ExecutionEnvironment, inputPath: String): DataSet[(Int, BDVector[Double])] = {
    env
      .readTextFile(inputPath)
      .map(s => {
        val line = s.replaceAll("[\\(\\)]", "").split(',')
        val point: BDVector[Double] = BDVector[Double](line.slice(0,line.length - 1).map(_.toDouble))
        val index = line.last.toInt
        (index, point)
      })
  }

  def getDataSet(env: ExecutionEnvironment, inputPath: String): DataSet[Vector] = {
    env
      .readTextFile(inputPath)
      .map(s => {
        val point: DenseVector = DenseVector(s.split(",").map(_.toDouble))
        point
      })
  }

  def getBreezeDataSet(env: ExecutionEnvironment, inputPath: String): DataSet[BDVector[Double]] = {
    env
      .readTextFile(inputPath)
      .map(s => {
        val point: BDVector[Double] = BDVector[Double](s.split(",").map(_.toDouble))
        point
      })
  }

  def getPointDataSet(env: ExecutionEnvironment, inputPath: String): DataSet[Vector] = getDataSet(env, inputPath)
  def getCentroidDataSet(env: ExecutionEnvironment, inputPath: String): DataSet[Vector] = getDataSet(env, inputPath)

  def getBreezePointDataSet(env: ExecutionEnvironment, inputPath: String): DataSet[BDVector[Double]] = getBreezeDataSet(env, inputPath)
  def getBreezeCentroidDataSet(env: ExecutionEnvironment, inputPath: String): DataSet[BDVector[Double]] = getBreezeDataSet(env, inputPath)



  // *************************************************************************
  //     EVALUATION METRICS
  // *************************************************************************

  def euclideanDistance(v1: Vector, v2: Vector): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" + s"=${v2.size}.")
    var squaredDistance = 0.0
    (v1, v2) match {
      case (v1: SparseVector, v2: SparseVector) =>
        val v1Values = v1.data
        val v1Indices = v1.indices
        val v2Values = v2.data
        val v2Indices = v2.indices
        val nnzv1 = v1Indices.length
        val nnzv2 = v2Indices.length

        var kv1 = 0
        var kv2 = 0
        while (kv1 < nnzv1 || kv2 < nnzv2) {
          var score = 0.0

          if (kv2 >= nnzv2 || (kv1 < nnzv1 && v1Indices(kv1) < v2Indices(kv2))) {
            score = v1Values(kv1)
            kv1 += 1
          } else if (kv1 >= nnzv1 || (kv2 < nnzv2 && v2Indices(kv2) < v1Indices(kv1))) {
            score = v2Values(kv2)
            kv2 += 1
          } else {
            score = v1Values(kv1) - v2Values(kv2)
            kv1 += 1
            kv2 += 1
          }
          squaredDistance += score * score
        }

      case (v1: SparseVector, v2: DenseVector) =>
        squaredDistance = sqdist(v1, v2)

      case (v1: DenseVector, v2: SparseVector) =>
        squaredDistance = sqdist(v2, v1)

      case (DenseVector(vv1), DenseVector(vv2)) =>
        var kv = 0
        val sz = vv1.length
        while (kv < sz) {
          val score = vv1(kv) - vv2(kv)
          squaredDistance += score * score
          kv += 1
        }
      case _ =>
        throw new IllegalArgumentException("Do not support vector type " + v1.getClass +
          " and " + v2.getClass)
    }
    squaredDistance
  }


  def sqdist(v1: SparseVector, v2: DenseVector): Double = {
    var kv1 = 0
    var kv2 = 0
    val indices = v1.indices
    var squaredDistance = 0.0
    val nnzv1 = indices.length
    val nnzv2 = v2.size
    var iv1 = if (nnzv1 > 0) indices(kv1) else -1

    while (kv2 < nnzv2) {
      var score = 0.0
      if (kv2 != iv1) {
        score = v2(kv2)
      } else {
        score = v1.data(kv1) - v2(kv2)
        if (kv1 < nnzv1 - 1) {
          kv1 += 1
          iv1 = indices(kv1)
        }
      }
      squaredDistance += score * score
      kv2 += 1
    }
    squaredDistance
  }
}
