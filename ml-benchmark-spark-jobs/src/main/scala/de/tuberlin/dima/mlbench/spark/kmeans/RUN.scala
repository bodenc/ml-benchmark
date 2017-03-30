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

package de.tuberlin.dima.mlbench.spark.kmeans

import breeze.linalg.DenseVector
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans => SparkKMeans}
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import breeze.linalg.{squaredDistance, DenseVector => BDVector, SparseVector => BSVector, Vector => BVector}
import de.tuberlin.dima.mlbench.spark.kmeans.optimizers.KMeans



object RUN {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("spark KMeanst")
    val sc = new SparkContext(conf)

    // assert(args.nonEmpty)

    val options = args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case Array(opt) => opt -> "true"
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap

    val inInitPath = options.get("inInitPath").get
    val inCentersPath = options.get("inCentersPath").get
    val inDataPath = options.get("inDataPath").get
    val outCentersPath = options.getOrElse("outputPath", "/benchmark/sparkOut")
    val numSplits = options.get("numSplits").get.toInt
    val numFeatures = options.get("numDimensions").get.toInt
    val numClusters = options.get("k").get.toInt
    val numIterations = options.get("iterations").get.toInt

    val method = options.getOrElse("method", "DEFAULT")
    val storageLevel = options.getOrElse("storage", "MEMORY_AND_DISK_SER")

    method match {
      case "DEFAULT" => {
        val data = KMeans.getBreezePointDataSet(sc, inDataPath, numSplits)

        storageLevel match{
          case "MEMORY_ONLY" => data.cache()
          case "MEMORY_ONLY_SER" => data.persist(StorageLevel.MEMORY_ONLY_SER)
          case "MEMORY_AND_DISK" => data.persist(StorageLevel.MEMORY_AND_DISK)
          case "MEMORY_AND_DISK_SER" => data.persist(StorageLevel.MEMORY_AND_DISK_SER)
          case "DISK_ONLY" => data.persist(StorageLevel.DISK_ONLY)
        }

        // Load the real centers i.e. generators centers
        val realCenters = KMeans.getBreezeCentroidDataSet(sc, inCentersPath, numSplits).cache()

        // Load the provided initial centroid RDD
        val initCenters = KMeans.getBreezeInitCentersDataSet(sc, inInitPath, numSplits).collect()

        // Train the model
        val finalCentroids  = KMeans.computeBreezeClustering(data, initCenters, numIterations)

        // Parallelize the evaluated centroids set and saveAsText to disk
        val toSave = sc.parallelize(finalCentroids, numSplits)
        toSave.saveAsTextFile(outCentersPath)
      }


      case "SPARK" => {
        // Load and parse the data
        val data = sc.textFile(inDataPath, numSplits)
        storageLevel match{
          case "MEMORY_ONLY" => data.cache()
          case "MEMORY_ONLY_SER" => data.persist(StorageLevel.MEMORY_ONLY_SER)
          case "MEMORY_AND_DISK" => data.persist(StorageLevel.MEMORY_AND_DISK)
          case "MEMORY_AND_DISK_SER" => data.persist(StorageLevel.MEMORY_AND_DISK_SER)
          case "DISK_ONLY" => data.persist(StorageLevel.DISK_ONLY)
        }
        val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble)))
        val realCenters = sc.textFile(inCentersPath)
        val parsedCenters = realCenters.map(s => Vectors.dense(s.split(',').map(_.toDouble)))

        val initCenters = sc.textFile(inInitPath)
        val parsedInit = initCenters
          .map(s =>
            Vectors.dense(s.replaceAll("[\\(\\)]", "").split(',').slice(0, s.split(',').length -1).map(_.toDouble)))
          .collect()

        // Create KMeans model with the provided initial centroids
        val model = new KMeansModel(parsedInit)
        // Cluster the data into two classes using KMeans
        val solver = new SparkKMeans()
          .setK(numClusters)
          .setInitialModel(model)
          .setMaxIterations(numIterations)

        // Run the optimization solver
        val outModel = solver.run(parsedData)

      }
      case _ => throw new IllegalArgumentException(s"$method is not a valid method argument. Job execution aborted.")
    }
    sc.stop()
  }

}
