package ground.spark

import ground.Logging
import org.apache.spark.scheduler._

class ProcessListener extends SparkListener with Logging{

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(applicationStart)
    log.info(s"Application ${applicationStart.appName} started ${applicationStart.time}")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    super.onStageCompleted(stageCompleted)
    log.info(s"Stage ${stageCompleted.stageInfo.name} completed ${stageCompleted.stageInfo.details}")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    super.onJobEnd(jobEnd)
    log.info(s"Job ${jobEnd.jobId} completed ${jobEnd.jobResult} at ${jobEnd.time}")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    super.onJobStart(jobStart)
    log.info(s"Job ${jobStart.jobId} started at ${jobStart.time}")
  }
}
