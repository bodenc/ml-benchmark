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

package de.tuberlin.dima.mlbench.flink.kmeans

import de.tuberlin.dima.mlbench.flink.kmeans.optimizers.KMeans
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.ml.math.Vector
import breeze.linalg.{Vector => BVector, DenseVector => BDVector, SparseVector => BSVector}


object RUN {

  def main(args: Array[String]): Unit = {
    assert(args.nonEmpty)

    val options = args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case Array(opt) => opt -> "true"
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap

    val degreeOfParallelism = options.get("degOfParall").get.toInt
    val inCentersPath = options.get("inCentersPath").get
    val inInitPath = options.get("inInitPath").get
    val inDataPath = options.get("inDataPath").get
    val outCentersPath = options.get("outputPath").get
    val numFeatures = options.get("numDimensions").get.toInt
    val numClusters = options.get("k").get.toInt
    val numIterations = options.get("iterations").get.toInt
    val method = options.get("method").get

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(degreeOfParallelism)
    println("Degree of Parallelism: " + degreeOfParallelism)

    env.registerType(breeze.linalg.DenseVector.zeros[Double](0).getClass)
    env.registerType(breeze.linalg.SparseVector.zeros[Double](0).getClass)
    env.registerType(breeze.collection.mutable.SparseArray.getClass)

    method match {
      case "DEFAULT" => {
        val points: DataSet[BDVector[Double]] = KMeans.getBreezePointDataSet(env, inDataPath)
        val centroids: DataSet[BDVector[Double]] = KMeans.getBreezeCentroidDataSet(env, inCentersPath)
        val initCentroids: DataSet[(Int, BDVector[Double])] = KMeans.getBreezeInitCentersDataSet(env, inInitPath)
        val finalCentroids: DataSet[(Int, BDVector[Double])] = KMeans.computeBreezeClustering(points, initCentroids, numIterations)
        finalCentroids.writeAsText(outCentersPath, FileSystem.WriteMode.OVERWRITE)

      }
      case "FLINK" => {
        val points: DataSet[Vector] = KMeans.getPointDataSet(env, inDataPath)
        val centroids: DataSet[Vector] = KMeans.getCentroidDataSet(env, inCentersPath)
        val initCentroids: DataSet[(Int, Vector)] = KMeans.getInitCentersDataSet(env, inInitPath)
        val finalCentroids: DataSet[(Int, Vector)] = KMeans.computeFlinkClustering(points, initCentroids, numIterations)
        finalCentroids.writeAsText(outCentersPath, FileSystem.WriteMode.OVERWRITE)
      }
      case _ => throw new IllegalArgumentException(s"$method is not a valid method argument. Job execution aborted.")
    }

    env.execute()
  }



}
