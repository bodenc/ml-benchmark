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

package config


import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{CopiedDataSet, DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.ExperimentSequence.{Parameter, SimpleParameters}
import org.peelframework.core.beans.experiment.{ExperimentSequence, ExperimentSuite}
import org.peelframework.core.beans.system.Lifespan
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.flink.beans.experiment.FlinkExperiment
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.HDFS2
import org.springframework.context.{ApplicationContext, ApplicationContextAware}
import org.springframework.context.annotation.{Bean, Configuration}


@Configuration
class ExperimentsFlinkKMeans extends ApplicationContextAware {


  var ctx: ApplicationContext = null
  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  @Bean(name = Array("flink.kmeans.production-scaling"))
  def `flink.kmeans.production-scaling`: ExperimentSuite = new ExperimentSuite(
    for {
      topXXX /*     */ <- Seq("all")
      dimensions /* */ <- Seq(100)
      k /*          */ <- Seq(30)
      size             <- Seq(5.0, 50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0)

    } yield new FlinkExperiment(
      name = s"kmeans.flink.train.$topXXX.$k.$dimensions.$size",
      command =
        s"""
           |--class dima.tu.berlin.benchmark.flink.kmeans.RUN                  \\
           |$${app.path.apps}/peel-bundle-flink-jobs-1.0-SNAPSHOT.jar           \\
           |--inCentersPath=$${system.hadoop-2.path.input}/train/$dimensions/centers/$size/$k  \\
           |--inInitPath=$${system.hadoop-2.path.input}/train/$dimensions/init/$size/$k  \\
           |--inDataPath=$${system.hadoop-2.path.input}/train/$dimensions/$size/$k  \\
           |--outputPath=$${system.hadoop-2.path.output}/benchmark/$topXXX/$k/$dimensions/$size        \\
           |--iterations=$${scale.iterations}                                   \\
           |--degOfParall=$${system.default.config.parallelism.total}           \\
           |--numDimensions=$dimensions                                            \\
           |--method=DEFAULT                                            \\
           |--k=$k
          """.stripMargin.trim,
      config = ConfigFactory.parseString(
        s"""
           |system.default.config.slaves            = $${env.slaves.$topXXX.hosts}
           |system.default.config.parallelism.total = $${env.slaves.$topXXX.total.parallelism}
           |scale.iterations                        = 5
          """.stripMargin.trim),
      runs = 1,
      runner = ctx.getBean("flink-1.0.3", classOf[Flink]),
      systems = Set(ctx.getBean("dstat-0.7.2", classOf[Dstat])),
      inputs = Set(ctx.getBean(s"dataset.kmeans.generated.$dimensions.$size.$k.$topXXX", classOf[DataSet])),
      outputs = Set(ctx.getBean("benchmark.output", classOf[ExperimentOutput]))
    )
  )



  @Bean(name = Array("flink.kmeans.strong-scaling"))
  def `flink.kmeans.strong-scaling`: ExperimentSuite = new ExperimentSuite(
    for {
      topXXX /*     */ <- Seq("all", "top024", "top018", "top012", "top010", "top008", "top006")
      dimensions /* */ <- Seq(100)
      k /*          */ <- Seq(30)
      size             <- Seq(200.0)

    } yield new FlinkExperiment(
      name = s"kmeans.flink.train.$topXXX.$k.$dimensions.$size",
      command =
        s"""
           |--class dima.tu.berlin.benchmark.flink.kmeans.RUN                  \\
           |$${app.path.apps}/peel-bundle-flink-jobs-1.0-SNAPSHOT.jar           \\
           |--inCentersPath=$${system.hadoop-2.path.input}/train/$dimensions/centers/$size/$k  \\
           |--inInitPath=$${system.hadoop-2.path.input}/train/$dimensions/init/$size/$k  \\
           |--inDataPath=$${system.hadoop-2.path.input}/train/$dimensions/$size/$k  \\
           |--outputPath=$${system.hadoop-2.path.output}/benchmark/$topXXX/$k/$dimensions/$size        \\
           |--iterations=$${scale.iterations}                                   \\
           |--degOfParall=$${system.default.config.parallelism.total}           \\
           |--numDimensions=$dimensions                                            \\
           |--method=DEFAULT                                            \\
           |--k=$k
          """.stripMargin.trim,
      config = ConfigFactory.parseString(
        s"""
           |system.default.config.slaves            = $${env.slaves.$topXXX.hosts}
           |system.default.config.parallelism.total = $${env.slaves.$topXXX.total.parallelism}
           |scale.iterations                        = 5
          """.stripMargin.trim),
      runs = 1,
      runner = ctx.getBean("flink-1.0.3", classOf[Flink]),
      systems = Set(ctx.getBean("dstat-0.7.2", classOf[Dstat])),
      inputs = Set(ctx.getBean(s"dataset.kmeans.generated.$dimensions.$size.$k.$topXXX", classOf[DataSet])),
      outputs = Set(ctx.getBean("benchmark.output", classOf[ExperimentOutput]))
    )
  )

}
