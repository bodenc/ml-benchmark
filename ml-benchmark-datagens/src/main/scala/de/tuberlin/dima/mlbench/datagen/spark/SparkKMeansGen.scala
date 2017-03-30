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

package de.tuberlin.dima.mlbench.datagen.spark
import org.apache.spark._
import scala.util.Random


object SparkKMeansGen {

  /**
    * Helper function to estimate the number of data points that need to be
    * generated in order to achieve a target data set size
    *
    * @param gigaSize
    * @param numFeatures
    * @param noisePerc
    * @return
    */
  def getDataSetSizes(gigaSize: Double, numFeatures: Int, noisePerc: Double = 0.0): (Int) = {
    // assumed the size model, evaluate the number of data points needed
    // Considering each point a Double, each row numFeaturesWide
    val byteSize: Double = math.ceil(gigaSize * 1024 * 1024 * 1024)
    val rowByteSize: Double = math.ceil(8 * numFeatures)
    val targetPointsNumber: Int = math.ceil(byteSize / rowByteSize).toInt

    targetPointsNumber
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KMeansDataGenerator")
    val sc = new SparkContext(conf)

    assert(args.nonEmpty)

    val options =  args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case Array(opt) => opt -> "true"
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap


    // get parameters from command line and fail if they aren't set properly
    // thus intentionally avoiding getOrElse as we do not want to default to some value
    val numSplits = options.get("numSplits").get.toInt
    val outCentersPath = options.get("centersPath").get
    val outDataPath = options.get("dataPath").get
    val outInitPath = options.get("initPath").get
    val size = options.get("size").get.toDouble
    val numDimensions = options.get("numDimensions").get.toInt
    val k = options.get("k").get.toInt
    require(k > 0, "Required a non-zero clusters number")

    // set experiment invariant parameters
    val noisePerc = 0.05
    val stddev = 0.1
    val range = 10 // centroid range
    val seed = 42
    val sigma_limit = 5.0

    val random: Random = new Random(seed)
    val absStdDev = stddev * range

    // randomly sample centroids
    val centroids: Array[Array[Double]] = uniformRandomCenters(random, k, numDimensions, range)
    val numTasks = numSplits*3*16
    val numDataPoints = getDataSetSizes(size, numDimensions, noisePerc)
    val pointsPerTask = numDataPoints / numTasks

    // randomly sample stdev for each cluster
    val sigma: Array[Double] = Array.ofDim[Double](k)
    for(i <- 0 until k){
      sigma(i) = random.nextDouble() * sigma_limit
    }

    System.err.println("NUM DATAPOINTS:   " + numDataPoints)
    System.err.println("NUM K DIMENSIONS:   " + centroids(0).length)

    val dataset = sc.parallelize(0 until numTasks, numTasks).flatMap(i => {
      val rand = new Random(seed + i)

      for (j <- 0 until pointsPerTask) yield {
        val centroidID = rand.nextInt(k)
        val centroid = centroids(centroidID)
        val noiseOrNot = rand.nextDouble()
        var vec:Array[Double] = Array.ofDim[Double](numDimensions)
        if (noiseOrNot >= 1.0 - noisePerc) {
          vec = for (x <- centroid) yield x + rand.nextDouble()*sigma_limit // generate random number for evey dimension
        } else {
          vec = for (x <- centroid) yield x + sigma(centroidID) * rand.nextGaussian() // sample gaussian with stdev
        }
        vec
      }
    })

    //transform data to plain csv
    val csvData = dataset.map(dataPoint =>{
      var outStr: String = ""
      for (i <- dataPoint.indices){
          outStr+=(dataPoint(i) + ",")
      }
      val resString = outStr.substring(0, outStr.size-1)

      resString
    })

    // sample initial centroids for kmeans and persist
    val initCentersData = dataset.takeSample(false, k, random.nextLong())
    sc.parallelize(centroids).map(_.mkString(",")).saveAsTextFile(outCentersPath)
    sc.parallelize(initCentersData, numSplits).map(_.mkString(",")).zipWithIndex().saveAsTextFile(outInitPath)

    // write out generated data
    //dataset.map(_.mkString(",")).saveAsTextFile(outDataPath)
    csvData.saveAsTextFile(outDataPath)
    sc.stop()
  }

  /**
    * Generates num sparse centers at random
    *
    * @param rnd
    * @param num
    * @param dimensions
    * @param range
    * @return
    */
  def uniformRandomCenters(rnd: Random, num: Int, dimensions: Int, range: Double): Array[Array[Double]] = {
    val halfRange = range / 2
    val points: Array[Array[Double]] = Array.ofDim[Double](num, dimensions)

    for(i <-0 until num){
      for(dim <- 0 until dimensions){
        points(i)(dim) = (rnd.nextDouble() * range) - halfRange
      }
    }

    return points
  }
}
