package ground

import ground.spark.SparkUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object Test {

  implicit val spark  = SparkUtils.sparkSession()

  /**
    * each trade has 3 rows and 3 columns
    */
  def prepareData() = {
   val mockRows =  Seq(
     Row("ENS_1", "T1", "t0", Array(0.01, 0.2, 0.3)),
     Row("ENS_1", "T1", "t1",  Array(0.01, 0.2, 0.3)),
     Row("ENS_1", "T1", "t2",  Array(0.01, 0.2, 0.3)),
     Row("ENS_1", "T2", "t0",  Array(0.01, 0.2, 0.3)),
     Row("ENS_1", "T2", "t1", Array(0.01, 0.2, 0.3)),
     Row("ENS_1", "T2", "t2", Array(0.01, 0.2, 0.3)),
     Row("ENS_1", "T3", "t0", Array(0.01, 0.2, 0.3)),
     Row("ENS_1", "T3", "t1", Array(0.01, 0.2, 0.3)),
     Row("ENS_1", "T3", "t2", Array(0.01, 0.2, 0.3)),
     Row("ENS_2", "T4", "t0", Array(0.01, 0.2, 0.3)),
     Row("ENS_2", "T4", "t1", Array(0.01, 0.2, 0.3)),
     Row("ENS_2", "T4", "t2", Array(0.01, 0.2, 0.3))
   )
    val gridSchema = new StructType()
      .add(StructField("ens", StringType))
      .add(StructField("trades", StringType))
      .add(StructField("tenor", StringType))
      .add(StructField("exposure_grid", ArrayType(FloatType), true))

    val mockRDD = spark.sparkContext.parallelize(mockRows)
    val mockDF = spark.createDataFrame(mockRDD, gridSchema)

    mockDF

  }
  import org.apache.spark.sql.functions.udf
  def elementSumArray =
     (first : Array[Double], second : Array[Double]) => (first, second).zipped.map( _ + _)


  def operation(df: DataFrame) = {
    import spark.implicits._
    //import spark.sqlContext.implicits._
   // val relDS = df.groupBy($"ens",$"trades")
   // spark.udf.register("mergegrids", new MergeGridsUDAF)
    val mergegrids = new MergeGridsUDAF(3)
   // val mergeUDF = udf(elementSumArray)
   // df.withColumn("merged", mergeUDF($"exposure_grids"))
    df.groupBy($"ens", $"trades", $"tenor").agg(mergegrids($"exposure_grid").as("mergegrids"))
   // df.createOrReplaceTempView("grids")
    //spark.sql("select exposure_grid from grids where ens = 'ENS_1'")
  }

  def main(args: Array[String]): Unit = {
    val mockDF = prepareData()
    //mockDF.show()
    operation(mockDF).show()
  }
}
