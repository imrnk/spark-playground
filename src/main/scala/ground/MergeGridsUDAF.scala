package ground

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

class MergeGridsUDAF(val scenarioCount : Int) extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = new StructType()
    .add(StructField("exposure_grids", ArrayType(FloatType), true))

  override def bufferSchema: StructType = inputSchema

  override def dataType: DataType = ArrayType(FloatType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new Array[Float](scenarioCount)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val inputArr = input.getAs[mutable.WrappedArray[Float]](0)
    val bufferArr  = buffer.getAs[mutable.WrappedArray[Float]](0)
    //buffer(0) = (bufferArr, inputArr).zipped.map(_ + _)
    //val arrayBuffer = buffer(0)
    /*for ( i <- 0 to scenarioCount - 1) {
      buffer.update(i, buffer.getFloat(i) + inputArr(i))
    }*/
    buffer(0) = inputArr
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buffer1Arr = buffer1.getAs[mutable.WrappedArray[Float]](0)
    val buffer2Arr  = buffer2.getAs[mutable.WrappedArray[Float]](0)
    //buffer1(0) = (buffer1Arr, buffer2Arr).zipped.map(_ + _)
    for(i <- buffer1Arr.indices){
      buffer1Arr.update(i, buffer1Arr(i) + buffer2Arr(2))
    }
    buffer1(0) = buffer1Arr
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getAs[mutable.WrappedArray[Float]](0)
  }
}
