package `package`.`with`.functions
import java.io.IOException

import io.minio.MinioClient
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext

class FunctionProvider {
  def checkFileType(inputtype:String):String = {
      inputtype.toLowerCase match {
        case "csv" => "csv"
        case "json" => "json"
        case "parquet" => "parquet"
        case "text" => "txt"
        case "avro" => "avro"
        case "orc" => "orc"
        case "custom" => "customtype"
        case _ => {
          println(s"File type not $inputtype supported, supported file types are csv, json, parquet, text, avro, orc and custom")
          sys.exit(0)
        }
      }
  }

  def checkLocalInputPath(inputpath:String, filetype:String):String = {
    if (filetype != "customtype") {
        if (new java.io.File(inputpath).isDirectory){      //read multiple files in a directory if absolute path of file given
          if(inputpath.endsWith("/"))
            return inputpath+"*."+filetype
          else
            return inputpath + "/*." + filetype
        }
        else if (new java.io.File(inputpath+"."+filetype).isFile)    //attach extension to file if possible
            return s"$inputpath.$filetype"
        else if (new java.io.File(inputpath).isFile) {              //check for file matching file type
          if (inputpath.contains(filetype))
            return inputpath
          else {
            println("Please input correct extension of file")
            sys.exit(0)
          }
        }
        else {
          println("Read path does not exist, please check that its a absolute path and try again")
          sys.exit(0)
        }
    }
    else if (filetype == "customtype") {
      if (new java.io.File(inputpath).isDirectory) {          //expects to read all in directory
        if(inputpath.endsWith("/"))
          return inputpath + "*"
        else
          return inputpath + "/*"}
      else if(new java.io.File(inputpath).isFile)          //expects to read the sole file given
        return inputpath
      else {
        println("Path does not exist, please check that its a absolute path and try again")
        sys.exit(0)
      }
    }
    "Unknown Error (checkLocal)"
  }

  def getFileTypeIndex(fileTypeInput:String): Integer = {
    val filetypelist = List("csv","json","parquet","txt","avro","orc","custom")

    for(filetypeL <- filetypelist){
      if(fileTypeInput.endsWith(filetypeL))
        return filetypelist.indexOf(filetypeL)
    }
    return (-1)
  }

  def getFileType(fileTypeIndex:Integer): String = {
    val filetypelist = List("csv","json","parquet","txt","avro","orc","custom")
    filetypelist(fileTypeIndex)
  }

  def checkObstoreInputPath(inputpath:String, filetype:String, spark:SQLContext):String = {
    val fsPath = new Path(inputpath)
    var noFromList = new Integer(0)

    noFromList = getFileTypeIndex(filetype)                       //get index to compare with read path which contains it

    try {
      val fs = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)   //causes exception if bucket cannot be found
      if(!inputpath.contains("/*")){    //getFileStatus cannot check for /*.filetype. If error, will get caught at Spark read operation
        try{
         fs.getFileStatus(fsPath)                     //checks for the file/directory. If file, requires extension
         println("Path for object store exist")
        }
        catch {
         case e : IOException => {
           println("Directory or file with that extension cannot be found on Object Store, exiting spark-job.")
           sys.exit(0)}
        }
      }
      if(filetype!="customtype") {
        if(inputpath.contains("."+filetype))           //checking to see if read path is a path to a file
          return inputpath
        else if(noFromList!=getFileTypeIndex(inputpath.takeRight(8)) && getFileTypeIndex(inputpath.takeRight(8))!=(-1)) {
          println(s"File type given is $filetype but extension contains "+getFileType(getFileTypeIndex(inputpath.takeRight(8)))+" extension")
          println("File type given does not match extension given, exiting spark-job")
          sys.exit(0)
        }
        else {
          if(inputpath.endsWith("/"))
            return inputpath + "*." + filetype
          else if(inputpath.endsWith("*"))
            return inputpath+"." + filetype
          else
            return inputpath + "/*." + filetype
          }
      }
      else if(filetype=="customtype") {
        if(inputpath.endsWith("*"))                                   //check for /*
          return inputpath
        else if (getFileTypeIndex(inputpath.takeRight(8))!=(-1))      //check for other filetype, can be commented to take in built-in file types
          return inputpath
        else {                                                        //read all in directory
          if(inputpath.endsWith("/"))
            return inputpath + "*"
          else
            return inputpath + "/*"
        }
      }
    }
    catch {
      case e: IOException => {
        println("Bucket cannot be found")
        sys.exit(0)
      }
      case _ : Throwable =>{
        println("Error in reading Object Store")
        println("Ensure object store path starts with s3a://bucketname")
        sys.exit(0)
      }
    }
    "Unknown Error (obstore)"
  }

  def createBucket(writepath: String, endpoint: String, access_key: String, secret_key: String): String = {
    val minioClient = new MinioClient(endpoint, access_key, secret_key)
    val splitone = writepath.split("://")
    val splittwo = splitone(1).split("/")
    /*the naming of the bucket is also checked at bucketExists, as such it accommodates for the checking of makeBucket too*/
    try {
      if (minioClient.bucketExists(splittwo(0)))
        return "Bucket exists. (F)"
      else {
        try {
          minioClient.makeBucket(splittwo(0))
          return "Bucket has been created successfully"
        } catch {
          case _: Throwable => {
            println("Bucket could not be created.")
            println("Unknown Error")
            sys.exit(1)
          }
        }
      }
    } catch {
      case _: Throwable =>
        println("Bucket name given is "+splittwo(0))
        println("Bucket could not be created as it does not follow bucket naming conventions.")
        println("For more information refer http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html")
        sys.exit(1)
    }
  }


  def checkAdditionArgs(filesystem: String, endpoint: String=null, access_key: String=null, secret_key: String=null,
                        args3: String, args4: String=null, writepath: String=null): Boolean = {
    filesystem match {
      case "localfs" => {
        val args3lower = args3.toLowerCase
        if(args3lower=="true" || args3lower=="false") {
          println("Multiline option for json read set to " + args3lower)
          return args3lower.toBoolean
        } else if(args3=="CreateBucket") {
          println("CreateBucket argument not available for local write paths")
          println("Multiline option remains true by default")
          return true
        } else {
          println("Argument given for multiline is invalid, multiline option remains true by default")
          return true
        }
      }
      case "s3a" => {
        if(args4==null) {
          val args3lower = args3.toLowerCase
          if (args3lower == "true" || args3lower == "false") {
            println("Multiline option for json read set to " + args3lower)
            return args3lower.toBoolean
          }
          else if (args3 == "CreateBucket") {
            println("Bucket will be created according to the write path given bucket name")
            println(createBucket(writepath, endpoint, access_key, secret_key))
            return true
          }
          else {
            println("Invalid 3rd argument is given. true false and CreateBucket is available for the 3th argument")
            println("Multiline option set to true by default")
            return true
          }
        }
        else {
          val args3lower = args3.toLowerCase
          val args4lower = args4.toLowerCase
          if ((args3lower == "true" || args3lower == "false") && (args4 == "CreateBucket")) {
            println("Multiline option for json read set to " + args3lower)
            println(createBucket(writepath, endpoint, access_key, secret_key))
            return args3lower.toBoolean
          }
          else if ((args4lower == "true" || args4lower == "false") && (args3 == "CreateBucket")) {
            println("Multiline option for json read set to " + args4lower)
            println(new FunctionProvider().createBucket(writepath, endpoint, access_key, secret_key))
            return args4lower.toBoolean
          }
          else {
            println("Invalid 3rd or 4th argument is given. true/false and CreateBucket are the available argument")
            println("Multiline option set to true by default")
            println("Bucket could not be created")
            return true
          }
        }
      }
      case _ => {
        println("filesystem mismatch")
        println("Unknown Error checkAddArgs")
        sys.exit(1)
      }
    }
  }
}
