import java.lang.Throwable

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object dataSourceReader {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
      .set("spark.hadoop.fs.s3a.access.key", "minio")
      .set("spark.hadoop.fs.s3a.secret.key", "minio123")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val spark = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("CustomSource")
      .config("type", "///")
      .getOrCreate()

    var filetype = new String

     try {
       filetype = args(0).toLowerCase match {
         case "csv" => "csv"
         case "json" => "json"
         case "parquet" => "parquet"
         case "text" => "text"
         case "avro" => "avro"
         case "custom" => "customtype"
         case _ => {
           println("File type not supported, supported file types are csv, json, parquet, text, avro and custom")
           sys.exit(0)}}
     }  catch{
       case _ : Throwable => {
         println("File type not given, please input a supported file type")
         sys.exit(0)
       }
     }

    println("File type given is:"+filetype)

    var inputpath = new String

    try{
      if(new java.io.File(args(1)).isDirectory())
        inputpath = args(1)+"/*."+filetype
      else if(new java.io.File(args(1)).isFile()) {
        if(args(1).contains(filetype))
          inputpath = args(1)
        else {
          println("Please input extension of file")
          sys.exit(0)
        }
      }
      else {
        println("Path does not exist, please check again")
        sys.exit(0)
      }
    }
    catch {
      case _: Throwable => {
        println("Input Path not given, please input a absolute path to folder")
        sys.exit(0)
      }
    }

    val data = spark.read.format(filetype).load("/home/yy/textFileRead/*.XPQZ")

    data.printSchema()
    data.show()

    //data.createOrReplaceTempView("tone")
    //spark.sql("select ID from tone").show()
    data.write.format("customtype").mode(SaveMode.Overwrite)
      .save("/home/yy/textFileSave/CustomDataSourceTest")

    //data.write.format("customtype").mode("ignore").save("s3a://spark-test/CustomDataSource")
  }
}
