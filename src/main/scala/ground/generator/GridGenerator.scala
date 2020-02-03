package ground.generator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

class GridGenerator(val scenario: Int, val tenor: Int, seed : Int) extends Serializable {

  def generate(trade : String): Seq[Row] = {
    /**
    Seq(Row(Array(1.1,2.3,3.4)),
    Row(Array(1.7,2.6,3.4)),
    Row(Array(4.4,5.1,6.2)))
    */

    val random = new scala.util.Random(seed)
    val random2 = new scala.util.Random(seed + 13)

    val grids = Range(0, scenario).map { is =>
      Row(trade, Range(0, tenor).map(it => random.nextInt(300) * random2.nextFloat).toArray)
    }
    //val gridRdd = session.sparkContext.parallelize(grids)
    //val gridSchema = new StructType().add(StructField("trades", StringType)).add(StructField("exposure_grid", ArrayType(FloatType), true))
   // session.createDataFrame(gridRdd, gridSchema)
    grids
  }


  private def print(grid: Array[Array[Float]]): Unit = {
    grid.foreach(row => println(row.mkString("[",",","]")))
  }

}

object GridGenerator {
 def apply(scenario : Int, tenor : Int, seed : Int) : GridGenerator = new GridGenerator(scenario, tenor, seed)
}
