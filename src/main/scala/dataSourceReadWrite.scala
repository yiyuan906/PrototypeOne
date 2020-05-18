import java.io.IOException

import `package`.`with`.functions.FunctionProvider
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object dataSourceReadWrite {
  def main(args: Array[String]): Unit = {
    if(args.length<3){
      println("Please enter at least 3 arguments in the order of filetype, readpath and writepath to use this program.")
      println("Example: ./spark-submit . . . path/to/jar filetype readpath writepath")
      sys.exit(1)
    }

    val endpoint = "http://127.0.0.1:9000"
    val access_key = "minio"
    val secret_key = "minio123"

    val conf = new SparkConf()
      .set("spark.hadoop.fs.s3a.endpoint", endpoint)
      .set("spark.hadoop.fs.s3a.access.key", access_key)
      .set("spark.hadoop.fs.s3a.secret.key", secret_key)
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val Spark = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("Datasource Read and Write")
      .getOrCreate()

    var fileType = new String
    var inputpath = new String
    var outputpath = new String
    val funcProvider = new FunctionProvider

    //checking of argument zero (file type)
    fileType = funcProvider.checkFileType(args(0));println("File type given is:"+fileType)

    //checking of argument one
    //If input given is /*.filetype on local system, it will get absolute path of files with that file type and use it as subsequent args.
    //To use /*.filetype, give absolute path for directory and the function will append it to the path after checking for it as directory.
    //It would be possible to use that on MinIO directly.
    if (!args(1).contains("s3a")){
        inputpath = funcProvider.checkLocalInputPath(args(1), fileType)
        println("Read path given is "+inputpath)
    }
    else if (args(1).contains("s3a")){
        inputpath = funcProvider.checkObstoreInputPath(args(1), fileType, Spark.sqlContext)
        println("Input path given is "+inputpath)
    }

    var overThreeArgsCheck = true                         //setting of multiline option with default as true

    if(args(2).contains("s3a://")){
      try {
        if(args.length>3) {
          args.length match {
            case 4 => overThreeArgsCheck = funcProvider.checkAdditionArgs("s3a",endpoint,access_key,secret_key,args3=args(3),writepath=args(2))
            case 5 => overThreeArgsCheck = funcProvider.checkAdditionArgs("s3a",endpoint,access_key,secret_key,args3=args(3),args4=args(4),writepath=args(2))
            case _ => {
              println("Amount of argument given is "+args.length)
              println("Argument given in order is:")
              args.foreach(argument=>println(argument))
              println("Input arguments given exceed what is required for the program with ObjectStore as write path. (Maximum 5)")
              println("Check the spark-submit sent again.")
              sys.exit(1)
            }
          }
        }

        val fsPath = new Path(args(2))
        val fs = fsPath.getFileSystem(Spark.sparkContext.hadoopConfiguration)     //checks for bucket
        outputpath=args(2)
        println("Bucket exists. (M)")
        println("Write path given is "+outputpath)
      } catch {
        case e: IOException => {println("Bucket cannot be found, please add CreateBucket argument to create bucket.");sys.exit(1)}
        case _ : Throwable =>{
          println("Write path given is "+args(2))
          println("Object Store cannot be found. Ensure path to ObjectStore uses s3a://bucketname...")
          sys.exit(1)}
      }
    }
    else {
        if(args.length>3){
          args.length match {
            case 4 => overThreeArgsCheck = funcProvider.checkAdditionArgs("localfs",args3=args(3))
            case _ => {
              println("Amount of argument given is "+args.length)
              println("Argument given in order is:")
              args.foreach(argument=>println(argument))
              println("Input arguments given exceed what is required for the program with local filesystem as write path. (Maximum 4)")
              println("Check the spark-submit sent again.")
              sys.exit(1)
            }
          }
        }
        outputpath=args(2)
        println("Write path given is "+outputpath)
    }

    //checks for if /*.filetype is given to path from local system
    if(outputpath.endsWith("."+fileType) && new java.io.File(outputpath).isFile && !outputpath.startsWith("s3a://")){
      println("Exiting spark-job due to write path being a file, please check again.")
      sys.exit(0)
    }

    try {
      if(fileType=="txt") fileType="text"
      val data = Spark.read.option("multiline", overThreeArgsCheck).format(fileType).load(inputpath)
      data.printSchema()
      data.show()
      try {
        data.write.format(fileType).mode(SaveMode.Overwrite).save(outputpath)
        println("Spark submit Successful")
      } catch {
        case e : IOException =>{
          println("Write path given cannot be written to, ensure write path is a proper path to a local filesystem or a path to " +
          "the object store.")
        }
        case _ : Throwable => {
          println(
            "Write path given cannot be written to, ensure that write path is a proper path to a local filesystem or a path to" +
              " the object store.")
          sys.exit(1)
        }
      }
    } catch {                                             // accounts for failed reads
      case _ : Throwable => {println("Read path given is not a proper directory holding the data, or data with this extension could not be found.")
        println("Please give a directory which holds the data or specify the file(s) by providing the extension by doing " +
        "filename.filetype or *.filetype which reads all of that file type. (only MinIO path supports /*.filetype as argument)")
        if(fileType=="json") println("Ensure multiline option for json is set properly, as this can also cause a read error.")
        sys.exit(0)
      }
    }
  }
}
