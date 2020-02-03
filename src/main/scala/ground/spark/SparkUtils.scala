package ground.spark

import ground.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils extends Logging{

  /**
    * Creates a SparkSession with Hive enabled
    */
  def sparkSession(): SparkSession = {
    val session = SparkSession.builder()
      .config("spark.master", "local")
      .master("local")
      .appName("playground")
      .enableHiveSupport()
      .getOrCreate()
    // Import the implicits, unlike in core Spark the implicits are defined
    // on the context.
    log.info(session.sparkContext.appName)
    session
  }

  def loadCSV(session: SparkSession, path : String) : DataFrame =
    session.read.option("header",true).csv(path)

}
