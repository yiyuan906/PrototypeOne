package `package`.`with`.provider

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class CustomProvider extends SchemaRelationProvider
  with CreatableRelationProvider
  with DataSourceRegister
  with RelationProvider {
  override def createRelation(sqlContextNS: SQLContext, parametersNS: Map[String, String]): BaseRelation = {
    new CustomRelation(sqlContextNS, parametersNS("path"), null)
  }

  override def createRelation(sqlContextS: SQLContext, parametersS: Map[String, String], schemaS: StructType): BaseRelation = {
    new CustomRelation(sqlContextS, parametersS("path"), schemaS)
  }

  override def createRelation(sqlContextW: SQLContext, modeW: SaveMode,
                              parametersW: Map[String, String], dataW: DataFrame): BaseRelation = {
    val path = parametersW("path")
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(sqlContextW.sparkContext.hadoopConfiguration)

    val copiedpath = new Path("/home/yy/textFileCopied")
    try { //if(path.contains("s3a"))
            fs.copyToLocalFile(fsPath, copiedpath)
    } catch {
      case _ : Throwable => println("Could not be copied")
    }


    modeW match {
      case SaveMode.Append => sys.error("Append mode is not supported by" + this.getClass.getCanonicalName); sys.exit(1)
      case SaveMode.Overwrite => fs.delete(fsPath, true)
      case SaveMode.ErrorIfExists =>sys.error("Given path: "+path+" already exist!"); sys.exit(1)
      case SaveMode.Ignore => sys.exit()
    }
    val customFormatRDD = dataW.rdd.map(row => {
      row.toSeq.map(value => value.toString).mkString("|")
    })
    customFormatRDD.saveAsTextFile(path)
    createRelation(sqlContextW, parametersW, dataW.schema)
  }

  override def shortName(): String = "customtype"
}