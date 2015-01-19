/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main.scala

import scala.collection.mutable.{ListBuffer, Queue}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

case class Person(name: String, age: Int)

object SparkSqlExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD
    val people = sc.textFile("/Users/blahiri/spark/examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    val people1 = sc.textFile("/Users/blahiri/spark/examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    people.registerTempTable("people")
    people.registerTempTable("people1")
   
    println("class of people = " + people.getClass + ", class of people1 = " + people1.getClass) //class reveals to be org.apache.spark.rdd.MapPartitionsRDD
    val union_all_people = people.unionAll(people1)
    println(union_all_people.getClass) //org.apache.spark.sql.SchemaRDD
    union_all_people.map(ua => "Name: " + ua(0)).collect().foreach(println)

    val intersection_people = people.intersection(people1)
    println(intersection_people.getClass)
    println(intersection_people.count())

    val union_people = people.union(people1) 
    //union_people.map(up => "Name: " + up(0)).collect().foreach(println)
    println(union_people.count()) //count gives 6, expected: 3
    println(union_people.getClass) //the class is org.apache.spark.rdd.UnionRDD, which is a subclass of org.apache.spark.rdd.RDD

    val people_dupes = sc.textFile("/Users/blahiri/spark/examples/src/main/resources/people_dupes.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    people_dupes.registerTempTable("people_dupes")
    val distinct_people = people_dupes.distinct()
    println(distinct_people.count())

    sc.stop()
  }
}
