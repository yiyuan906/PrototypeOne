# Prototype One

The main goal of this program is to allow files to be loaded to and from an Object Store, the Object Store being MinIO. 
Spark in this program serves as the intermediary between the local filesystem (user's computer) and MinIO, doing the reading and writing of the files.
So while spark do read the data, the program does not process the data and it is directly read and written to the given write path.

## Description of the program 

`csv` `json` `parquet` `text` `avro` `orc` `custom`   - file type supported

`/absolute/path/to/file/...`  - path from local filesystem

`s3a://bucketname/dir/...`    - path from Object Store

Spark has to be setup by downloading the necessary jar files to get the connection between itself and MinIO. 
This program makes use of scala with sbt and requires sbt to compile the program for a jar file which can be submitted for a spark job.
The program when used in spark-submit will then require arguments for it to be used. For example `./spark-submit /home/yy/IntelliJStore/PrototypeOne/target/scala-2.11/prototypeone_2.11-0.1.jar json s3a://spark-test/records.json /home/yy/ToDownload/single/json`

The program mainly does checking of the file type and paths provided for the local filesystem and MinIO, and would execute the spark-job if the arguments given are plausible.
The arguments which can be given upon spark-submit is limited to five in this order, `file type`, `read path`, `write path`, `true/false/CreateBucket`(Optional), `CreateBucket/true/false`(Optional).
The spark-submit with a local path as the write path is limited to four and the fourth argument is limited to true/false.
The spark-submit with MinIO as the write path is limited to five and is available to all the optional arguments.

The `true/false` argument given will set the multiline option that can be set for json file type, by default it is set to true.
The `CreateBucket` argument given will allow a bucket to be created when writing to the Object Store. 
If given `s3a://bucket123/dir/files` as the write path with `CreateBucket` as an argument, the bucket will be created as `bucket123` if it does not exists.

This program being allowed one read path only does not means that it is limited to reading of one file only, it is able to read all the files with the given extension if specified a directory for the read path.
`/path/to/dir/*.json` is used to get all the files in a directory under that json extension. 
However, inputting `*.filetype` at the end for a local path when doing the spark-submit will result in getting all the available files with that extension under that directory as absolute paths for the arguments. 
As such, `*.filetype` is appended if the read path given is checked to be a directory. 
This is not a issue if it is done for the read path of MinIO. `s3a://bucket123/dir/*.filetype` is acceptable and will not gather all the available files as individual paths.
To counteract this, the spark-job will stop if it checks that the argument given for the write path is a file from the local filesystem or if the argument exceeds 4 in this situation.

Except `avro`, all the filetypes listed can be submitted using spark-job if it meets the minimum argument requirement of `file type`, `read path` and `write path`.
`avro` file type would require `--packages org.apache.spark:spark-avro_2.11:2.4.5` to be added behind `./spark-submit` for it to be read.

The `custom` file type is a custom datasource, as of now it just maps the data as it is without doing any proccessing on the data for its read and write.
But it can be seen that proccessing can be added on top of the data for when it is read/written.

## Setup used

- VM uses ubuntu-18.04.4
- IntelliJ IDEA used as the IDE
- java -version `openjdk version "1.8.0_252"`

### MinIO server setup
Install MinIO Server from [here](https://docs.min.io/docs/minio-quickstart-guide).

The endpoint, access key and secret key used in this program is hardcoded to https://127.0.0.1:9000, minio and minio123 respectively.
To setup the access key and secret key when starting up the server, do `MINIO_ACCESS_KEY=minio MINIO_SECRET_KEY=minio123 ./minio server /path/to/local/miniostorage`

This settles the setup of MinIO. To check, go to https://127.0.0.1:9000 and enter the access key and secret key to enter.
(If endpoint differs, the program needs to change its endpoint accordingly as well.)

### Spark setup
Download Apache Spark version `spark-2.4.5-bin-hadoop2.7.tgz` from [here](https://spark.apache.org/downloads.html).

The dependencies required for Spark and MinIO connection are:
  - [`Hadoop AWS 2.7.3`](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.3)
  - [`HttpClient 4.5.3`](https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient/4.5.3)
  - [`Joda Time 2.9.9`](https://mvnrepository.com/artifact/joda-time/joda-time/2.9.9)
  - [`AWS SDK For Java Core 1.11.712`](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-core/1.11.712)
  - [`AWS SDK For Java 1.11.712`](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.11.712)
  - [`AWS Java SDK For AWS KMS 1.11.712`](http://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-kms/1.11.712)
  - [`AWS Java SDK For Amazon S3 1.11.712`](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3/1.11.712)
  - [`AWS SDK For Java 1.7.4`](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.7.4)

The dependencies required for the program's MinIO are:
  - [`Simple XML 2.7.1`](https://mvnrepository.com/artifact/org.simpleframework/simple-xml/2.7.1)
  - [`Minio 7.0.2`](https://mvnrepository.com/artifact/io.minio/minio/7.0.2)
  - [`Guava: Google Core Libraries For Java 28.2-jre`](https://mvnrepository.com/artifact/com.google.guava/guava/28.2-jre)
  
Extract the `spark-2.4.5-bin-hadoop2.7.tgz` tar ball in the directory where Spark is suppose to be installed. 
Afterwards, move all the dependency jar files downloaded previously into `/path/to/spark-2.4.5-bin-hadoop2.7/jars` directory.

### IntelliJ IDEA and sbt
- Download this project using git clone.
- Download IntelliJ IDEA and sbt.
- Use `Open or Import` under IntelliJ idea to open up the downloaded file from git clone.
- When the file has been loaded fully, cd to the absolute path of this project and enter `sbt package`.

A jar file will then appear under target in the project which will be the jar file that is used for spark-submit.
This jar is referenced in the spark-submit by using the absolute path to it.

## Running the program
With that setup all done, go to the bin where spark has been installed `/path/to/spark-2.4.5-bin-hadoop2.7/bin` and try it.

Single File Read Write Example:`./spark-submit --class dataSourceReadWrite /home/absolute/path/to/jarfile.jar filetype /path/to/read/from/file.filetype s3a://path/to/write/to/dir`

Multiple File Read Write Example:`./spark-submit --class dataSourceReadWrite /home/absolute/path/to/jarfile.jar filetype s3a://path/to/read/from/ /path/to/write/to/dir`

## Additional notes

### Two known issues
Local filesystem path which ends with `*.filetype` when entered on spark-submit, results in getting multiple paths to files as arguments if there are multiple files with that extension.

Spark write even when using `try catch` cannot catch the exception from main if directory given cannot be created on local filesystem.

### json files
Incorrect multiline option set might result in incorrect reading of json data. For a single json file case, if the json file is a multiline type, a error might occur when trying to read it if the setting is set to false. In the sample data provided, records.json is the multiline type json file.

 
