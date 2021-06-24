package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import sys.process._

object SparkObj {
  case class schema(txno:String, txndate:String, custno:String, amount:String, category:String,product:String, city:String, state:String, spendby:String)
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark=SparkSession.builder().getOrCreate()
					import spark.implicits._
					/*val file1=args(0)
					val file3=args(1)
					val file4=args(2)
					val file5=args(3)
					val file6=args(4)
					val dest=args(5)
					 
					
					println
					println("===========================scala list====================")
					println

					val list_int=List(1,4,6,7)    
					val list_itr=list_int.map(x=>x+2)
					list_itr.foreach(println)

					println
					println("===========================string filter list====================")
					println

					val list_str=List("Zeyobron", "Zeyo","analytics")
					val filter_list=list_str.filter(x=>x.contains("Zeyo"))
					filter_list.foreach(println)


					println
					println("===========================read file as rdd and filter====================")
					println

					val file_rdd=sc.textFile(file1)
					val filtr_rdd= file_rdd.filter(x=>x.contains("Gymnastics"))
					filtr_rdd.take(5).foreach(println)

					println
					println("===========================schema rdd imposing case class and columns filter====================")
					println
					
					val split_rdd=file_rdd.map(x=>x.split(","))
					val schema_rdd= split_rdd.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
					val filter_schema=schema_rdd.filter(x=>x.product.contains("Gymnastics"))
					filter_schema.take(5).foreach(println)
					
					println
					println("===========================row rdd and index based filters====================")
					println
					
					val row_rdd=split_rdd.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
					val index_filter=row_rdd.filter(x=>x(8).toString().contains("cash"))
					index_filter.take(5).foreach(println)
					
					println
					println("===========================dataframe creation from schema and row rdd====================")
					println

					val schema_df=schema_rdd.toDF()
					
					println("====schema df===")
					schema_df.show(5)
					
						val schema1=StructType(Array(

						StructField("txnno",StringType,true),
						StructField("txndate",StringType,true),
						StructField("custno",StringType,true),
						StructField("amount", StringType, true),
						StructField("category", StringType, true),
						StructField("product", StringType, true),
						StructField("city", StringType, true),
						StructField("state", StringType, true),
						StructField("spendby", StringType, true)
				  ))
				  
				  println("-======row df====")
				  val row_df=spark.createDataFrame(row_rdd, schema1)
				  row_df.show(5)
				  
				  
				  println
					println("===========================seamless csv read====================")
					println
				  
//					val csv_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///D:/training_tasks/data/file3.txt")
					val csv_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(file3)
					csv_df.show(5)
					
					 println
					println("===========================seamless json and parquet read====================")
					println
					
//					val json_df=spark.read.format("json").load("file:///D:/training_tasks/data/file4.json")
					val json_df=spark.read.format("json").load(file4)
					json_df.show(5)
					
//					val parquet_df=spark.read.load("file:///D:/training_tasks/data/file5.parquet")
					val parquet_df=spark.read.load(file5)
					parquet_df.show(5)
					
					
					println
					println("===========================seamless xml read====================")
					println
					
//					val xml_df=spark.read.format("com.databricks.spark.xml").option("rowTag","txndata").load("file:///D:/training_tasks/data/file6")
					val xml_df=spark.read.format("com.databricks.spark.xml").option("rowTag","txndata").load(file6)
					xml_df.show(5)
					
					println
					println("===========================union all data frames created====================")
					println
					
					val union_df=schema_df.union(row_df).union(csv_df).union(json_df).union(parquet_df).union(xml_df)
					
					union_df.show(5)
					
					println
					println("===========================adding columns and filtering the data====================")
					println
					
					val year_status_filter_df=union_df.withColumn("year",expr("split(txndate,'-')[2]"))
                                  					.withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
                                  					.filter(col("txno")<50000)
                                  					
          year_status_filter_df.show(5)
          
          println
					println("===========================writing to local as avro and partioned by category column in append mode====================")
					println				
					      
          
//          year_status_filter_df.write.format("com.databricks.spark.avro").partitionBy("category").mode("append").save("file:///D:/training_tasks/data/avro_revision")
          year_status_filter_df.write.format("com.databricks.spark.avro").partitionBy("category").mode("append").save(dest)
					
					
					
					
					    


*/


val txndf = spark.read.format("csv").option("delimiter","~").load("file:///D://training_tasks//data//devices.json")
//txndf.printSchema()
//txndf.show(5,false)



val weblogschema = StructType(Array(
					StructField("device_id", StringType, true),
					StructField("device_name", StringType, true),
					StructField("humidity", StringType, true),
					StructField("lat", StringType, true),
					StructField("long", StringType, true),    
					StructField("scale", StringType, true),
					StructField("temp", StringType, true),
					StructField("timestamp", StringType, true),
					StructField("zipcode", StringType, true)));  
			
			
//		txndf.withColumn("_c0",from_json(col("_c0"),weblogschema)).select("_c0.*").show(5,false)
		
		val jsondf = spark.read.format("json").option("multiLine","true").load("file:///D:/training_tasks/data/zeyodata.json")
		
		jsondf.show(10)
		jsondf.printSchema()
		
		jsondf.select("No","year","address.*").show()

			


	}
}