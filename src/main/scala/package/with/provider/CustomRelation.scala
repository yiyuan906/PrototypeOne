package `package`.`with`.provider

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class CustomRelation(sqlContextBack:SQLContext,pathBack:String, schemaBack:StructType)
  extends BaseRelation with PrunedFilteredScan{
  var passedSchema = new StructType()

  if(schemaBack == null) passedSchema = StructType(StructField("Value", StringType)::Nil)
  else passedSchema = schemaBack

  override def sqlContext: SQLContext = sqlContextBack

  override def schema: StructType = passedSchema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val data=sqlContextBack.sparkContext.textFile(pathBack).map(data=>Row(data))
    data
  }
}