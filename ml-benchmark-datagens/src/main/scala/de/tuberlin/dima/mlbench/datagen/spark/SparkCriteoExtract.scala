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

import java.nio.charset.Charset

import scala.util.hashing.MurmurHash3
import org.apache.spark.{SparkConf, SparkContext, _}


/**
  * Transform the Crito click log data to LibSVM data file with One Hot encoding of categorical features
  * based on  https://github.com/citlab/PServer/blob/master/preprocessing/criteo/src/main/scala/de/cit/pserver/CriteoPreprocessingJob.scala
  */

object SparkCriteoExtract {

  val Seed = 0

  // Properties of the Criteo Data Set
  val NUM_LABELS = 1
  val NUM_INTEGER_FEATURES = 13
  val NUM_CATEGORICAL_FEATURES = 26
  val NUM_FEATURES = NUM_LABELS + NUM_INTEGER_FEATURES + NUM_CATEGORICAL_FEATURES

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Feature Hashing Criteo Data Set")
    val spark = new SparkContext(conf)

    assert(args.nonEmpty)

    val options =  args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case Array(opt) => opt -> "true"
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap

    val inputPath= options.get("inputPath").get
    val outputPath = options.get("outputPath").get
    val numFeatures = options.get("numFeatures").get.toInt
    val percDataPoints = options.get("percDataPoints").get.toDouble
    var nSamples = options.get("numDataPoints").get.toInt

    val data = readInput(spark, inputPath)
    val numDataPoints: Int = (nSamples * percDataPoints).toInt

    // sub- or supersample the data for experiments
    val input = if(percDataPoints != 1.0) {
      data.sample(true, percDataPoints, 42)
    } else {
      data
    }
    nSamples = numDataPoints

    // compute mean for integer features
    val broadcastMean = spark.broadcast(input.map(input => input._2).reduce((integerArrayLeft, integerArrayRight) =>
      (integerArrayLeft, integerArrayRight).zipped map (_ + _)).map(_.toDouble / nSamples))

    // compute standard deviation for integer features
    val broadcastStdDeviation = spark.broadcast(input.map(input => input._2.zip(broadcastMean.value).map(x => math.pow(x._1 - x._2, 2))).reduce((integerArrayLeft, integerArrayRight) =>
      (integerArrayLeft, integerArrayRight).zipped map (_ + _))
      .map(dev => math.sqrt(dev / nSamples)))


    /*
    remove mean and scale to unit variance (standard deviation), hash features onto numFeatures
    dimensions
    */
    val transformedFeatures = input.map(x => {


      val label = x._1
      val intFeatures = x._2
      val catFeatures = x._3

      val normalizedIntFeatures = (intFeatures, broadcastMean.value, broadcastStdDeviation.value).zipped.toList.map {
        case (feature, mean, variance) => (feature - mean) / variance
      }

      val hashedIndices = catFeatures
        .filter(!_.isEmpty)
        .map(murmurHash(_, 1, numFeatures))
        .groupBy(_._1)
        .map(colCount => (colCount._1 + NUM_INTEGER_FEATURES + 1 , colCount._2.map(_._2).sum))
        .filter(_._2 != 0)
        .toSeq.sortBy(_._1)

      val intStrings = for ((col, value) <- 1 to normalizedIntFeatures.size zip normalizedIntFeatures) yield s"$col:$value"
      val catStrings = for ((col, value) <- hashedIndices) yield s"$col:$value"


      var retString = label.toString + " "

           if(label <= 0){
              retString  = "-1 "
            }
            else{
              retString  = "1 "
            }

      retString + (intStrings ++ catStrings).mkString(" ")
    })


    // output stats for logging
    Console.err.println("\n Number of Data Partitions :  " + transformedFeatures.getNumPartitions + "\n")
    Console.err.println("\n Number of Data Points :  " + transformedFeatures.count + "\n")
    Console.err.println("\n nSamples :  " + nSamples + "\n")
    Console.err.println("\n percentage :  " + percDataPoints + "\n")

    transformedFeatures.saveAsTextFile(outputPath)
    spark.stop()
  }


  /**
    * read the input file and separate label, integer and categorical features
    * @param sc
    * @param input
    * @param delimiter
    * @return
    */
  def readInput(sc: SparkContext, input: String, delimiter: String = "\t") = {
    sc.textFile(input) map { line =>
      val features = line.split(delimiter, -1)

      val label = features.take(NUM_LABELS).head.toInt
      val integerFeatures = features.slice(NUM_LABELS, NUM_LABELS + NUM_INTEGER_FEATURES)
        .map(string => if (string.isEmpty) 0 else string.toInt)
      val categorialFeatures = features.slice(NUM_LABELS + NUM_INTEGER_FEATURES, NUM_FEATURES)

      // add dimension so that similar values in diff. dimensions get a different hash
      for(i <-  categorialFeatures.indices){
        categorialFeatures(i) = i + ":" + categorialFeatures(i)
      }

      (label, integerFeatures, categorialFeatures)
    }
  }

  /**
    * awkward hash function
    * @param feature
    * @param count
    * @param numFeatures
    * @return
    */

  private def murmurHash(feature: String, count: Int, numFeatures: Int): (Int, Int) = {
    val hash = MurmurHash3.bytesHash(feature.getBytes(Charset.forName("UTF-8")), Seed)
    val index = scala.math.abs(hash) % numFeatures


    val value = if (hash >= 0) count else -1 * count
    (index, value)
  }




}
