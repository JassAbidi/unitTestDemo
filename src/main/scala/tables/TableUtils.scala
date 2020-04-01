package tables

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.PartitionUtils.inferPartition
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object TableUtils {

  def filterByPartitionsPredicates(fileIndex: InMemoryFileIndex, partitionFilters: Seq[Expression])(implicit sparkSession: SparkSession): Array[String] = {
    val userSpecifiedDataTypes = fileIndex.partitionSpec().partitionColumns.map(f => (f.name, f.dataType)).toMap
    val rows = fileIndex.inputFiles.map { file =>
      val parsedPartitions = inferPartition(sparkSession, new Path(file), fileIndex.rootPaths.toSet, userSpecifiedDataTypes)
      Row.fromSeq(parsedPartitions._1.get.literals.map { l => if (l.dataType == StringType) l.value.toString else l.value } ++ Seq(file))
    }

    val filenameSchema = StructType(Array(StructField("fileName", StringType, false)))
    val finalSchema = StructType(fileIndex.partitionSpec().partitionColumns ++ filenameSchema)

    val columnFilter = new Column(partitionFilters.reduceLeftOption(And).getOrElse(Literal(true)))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows), finalSchema)
      .filter(columnFilter)
      .select("fileName")
      .collect()
      .map(r => r.getAs[String]("fileName"))
  }

  def remove(file: String)(implicit sparkSession: SparkSession): Unit = {
    val sessionHadoopConf = sparkSession.sessionState.newHadoopConf()
    val path = new Path(file)
    val fs = path.getFileSystem(sessionHadoopConf)
    fs.delete(path, false)
  }


  def move(sourceFile: String, destination: String)(implicit sparkSession: SparkSession): Unit = {
    val sessionHadoopConf = sparkSession.sessionState.newHadoopConf()
    val sourceFilePath = new Path(sourceFile)
    val fs = sourceFilePath.getFileSystem(sessionHadoopConf)
    fs.moveFromLocalFile(sourceFilePath, new Path(destination))
  }

  def splitMetadataAndDataPredicates(condition: Expression, partitionColumns: Seq[String])(implicit sparkSession: SparkSession): (Seq[Expression], Seq[Expression]) = {
    val predicates = splitConjunctivePredicates(condition)
    predicates.partition(isPredicateMetadataOnly(_, partitionColumns))
  }

  protected def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  def isPredicateMetadataOnly(condition: Expression, partitionColumns: Seq[String])(implicit sparkSession: SparkSession): Boolean = {
    val nameEquality = sparkSession.sessionState.analyzer.resolver
    condition.references.forall { r =>
      partitionColumns.exists(nameEquality(r.name, _))
    }
  }
}
