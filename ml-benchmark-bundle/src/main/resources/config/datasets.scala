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

package config;


import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{CopiedDataSet, DataSet, ExperimentOutput, GeneratedDataSet}
import org.peelframework.flink.beans.system.Flink
import org.peelframework.flink.beans.job.FlinkJob
import org.peelframework.spark.beans.job.SparkJob
import org.peelframework.spark.beans.system.Spark
import org.peelframework.hadoop.beans.system.HDFS2
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}


@Configuration
class datasets extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ****************************************************************
  // Data Generator Jobs
  // ****************************************************************

  // Criteo Feature Hashing
  def sparkCriteoExtractJob(name: String, system: String, inputPath: String, numFeatures: Int, perc: Double): SparkJob = new SparkJob(
    timeout /**/ = 10000L,
    runner /* */ = ctx.getBean("spark-1.6.2", classOf[Spark]),
    command /**/ =
      s"""
         |--class dima.tu.berlin.generators.spark.SparkCriteoExtract                     \\
         |$${app.path.datagens}/peel-bundle-datagens-1.0-SNAPSHOT.jar                        \\
         |--inputPath=$inputPath                                                              \\
         |--outputPath=$${system.hadoop-2.path.input}/train                     \\
         |--numFeatures=$numFeatures                          \\
         |--percDataPoints=$perc   \\
         |--numDataPoints=1150708097    \\
      """.stripMargin.trim
  )

  // KMeans Data Generator
  def kmeansDataGenerationJob(name: String, system: String, numFeatures: Int, size: Double, noisePerc: Double, k: Int, numSplits:Int): SparkJob = new SparkJob(
    timeout /**/ = 10000L,
    runner /* */ = ctx.getBean("spark-1.6.2", classOf[Spark]),
    command /**/ =
      s"""
         |--class dima.tu.berlin.generators.spark.SparkKMeansGen                     \\
         |$${app.path.datagens}/peel-bundle-datagens-1.0-SNAPSHOT.jar                        \\
         |--centersPath=$${system.hadoop-2.path.input}/train/$numFeatures/centers//$size/$k  \\
         |--dataPath=$${system.hadoop-2.path.input}/train/$numFeatures/$size/$k                       \\
         |--initPath=$${system.hadoop-2.path.input}/train/$numFeatures/init/$size/$k                  \\
         |--numDimensions=$numFeatures                          \\
         |--noisePerc=$noisePerc                          \\
         |--k=$k                          \\
         |--numSplits=$numSplits    \\
         |--size=$size
      """.stripMargin.trim
  )

  // ****************************************************************
  // Generated DataSets
  // ****************************************************************

  def criteoDataSet(name: String, system: String, inputPath: String, numFeatures: Int, perc: Double): DataSet = new GeneratedDataSet(
    src /**/ = sparkCriteoExtractJob(name, system, inputPath, numFeatures, perc),
    dst /**/ = s"$${system.hadoop-2.path.input}/train",
    fs /* */ = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  def kmeansDataSet(name: String, system: String, numFeatures: Int, size: Double, noisePerc: Double, k: Int, numSplits:Int): DataSet = new GeneratedDataSet(
    src /**/ = kmeansDataGenerationJob(name, system, numFeatures, size, noisePerc, k, numSplits),
    dst /**/ = s"$${system.hadoop-2.path.input}/train/"+numFeatures,
    fs /* */ = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )


  // ****************************************************************
  // Output Directory
  // ****************************************************************

  @Bean(name = Array("benchmark.output"))
  def `wordcount.output`: ExperimentOutput = new ExperimentOutput(
    path = "{system.hadoop-2.path.input}/benchmark/",
    fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  // ****************************************************************
  // Generated DataSet Beans
  // ****************************************************************




  // ****************************************************************
  // COST Experiments
  // ****************************************************************

  val localCOSTDataSets = "/data/cost/"

  @Bean(name = Array("dataset.copied.libsvm.10"))
  def `dataset.copied.libsvm.10`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata10",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.copied.libsvm.100"))
  def `dataset.copied.libsvm.100`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata100",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.copied.libsvm.1000"))
  def `dataset.copied.libsvm.1000`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata1k",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.copied.libsvm.10000"))
  def `dataset.copied.libsvm.10000`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata10k",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.copied.libsvm.100000"))
  def `dataset.copied.libsvm.100000`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata100k",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.copied.libsvm.1000000"))
  def `dataset.copied.libsvm.1000000`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata1m",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.copied.libsvm.10000000"))
  def `dataset.copied.libsvm.10000000`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata10m",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.copied.libsvm.100000000"))
  def `dataset.copied.libsvm.100000000`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata100m",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.copied.libsvm.1000000000"))
  def `dataset.copied.libsvm.1000000000`: DataSet = new CopiedDataSet(
    src = localCOSTDataSets + "libdata1000m",
    dst  = s"$${system.hadoop-2.path.input}/train",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )


  // ****************************************************************
  // Crite Data Set
  // ****************************************************************

  val inputCriteo = "file:///data/criteo/"






  // ****************************************************************
  // KMeans Data Sets
  // ****************************************************************



  // KMeans Production Scaling with Data Size = 5.0 .. 1000 GB, K=30, all nodes

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.5.0.30.all"))
  def `dataset.kmeans.generated.100.5.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 5.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.50.0.30.all"))
  def `dataset.kmeans.generated.100.50.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 50.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.100.0.30.all"))
  def `dataset.kmeans.generated.100.100.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 100.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.all"))
  def `dataset.kmeans.generated.100.200.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.300.0.30.all"))
  def `dataset.kmeans.generated.100.300.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 300.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.400.0.30.all"))
  def `dataset.kmeans.generated.100.400.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 400.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.500.0.30.all"))
  def `dataset.kmeans.generated.100.500.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 500.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.600.0.30.all"))
  def `dataset.kmeans.generated.100.600.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 600.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.700.0.30.all"))
  def `dataset.kmeans.generated.100.700.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 700.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.800.0.30.all"))
  def `dataset.kmeans.generated.100.800.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 800.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.900.0.30.all"))
  def `dataset.kmeans.generated.100.900.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 900.0, 0.1, 30, 30)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.1000.0.30.all"))
  def `dataset.kmeans.generated.100.1000.0.30.all`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 1000.0, 0.1, 30, 30)





  // KMeans Strong Scaling with data set size = 200 GB, K = 10

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.top024"))
  def `dataset.kmeans.generated.100.200.0.30.top024`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 24)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.top018"))
  def `dataset.kmeans.generated.100.200.0.30.top018`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 18)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.top012"))
  def `dataset.kmeans.generated.100.200.0.30.top012`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 12)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.top010"))
  def `dataset.kmeans.generated.100.200.0.30.top010`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 10)

  // KMeans Data Set with 100 Dimensions
  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.top006"))
  def `dataset.kmeans.generated.100.200.0.30.top006`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 6)

  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.top004"))
  def `dataset.kmeans.generated.100.200.0.30.top004`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 4)

  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.top002"))
  def `dataset.kmeans.generated.100.200.0.30.top002`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 2)

  @Bean(name = Array("dataset.kmeans.generated.100.200.0.30.top008"))
  def `dataset.kmeans.generated.100.200.0.30.top008`: DataSet =
    kmeansDataSet("kmeans", "spark", 100, 200.0, 0.1, 30, 8)

}