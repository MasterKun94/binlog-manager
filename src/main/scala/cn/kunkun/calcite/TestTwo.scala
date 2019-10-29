package cn.kunkun.calcite

import cn.kunkun.binlog.ImplicitUtil.str2Symbol
import cn.kunkun.binlog.Statistics.PrimaryKey
import cn.kunkun.binlog._
import cn.kunkun.calcite.ImplicitUtil._
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.sql.dialect.CalciteSqlDialect
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.Frameworks

object TestTwo extends App {

  val registry = Registry.default

  registry.registryTable(
    BTable("test", "user")(
      BColumn("id", ColumnType.LONG),
      BColumn("name", ColumnType.VARCHAR),
      BColumn("age", ColumnType.LONG),
      BColumn("hobby", ColumnType.VARCHAR)
    )(
      PrimaryKey("id")
    ))
  println(registry)

  val schemaPlus = Frameworks.createRootSchema(true)
  schemaPlus.registryFrom(registry)
  val frameworkConfig = Frameworks.newConfigBuilder()
    .defaultSchema(schemaPlus)
    .build()
  val parserConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig)
  parserConfig.setCaseSensitive(false).setConfig(parserConfig.build())
  val planner = Frameworks.getPlanner(frameworkConfig)
  val sqlNode = planner.parse(
    """
      |delete from "test"."user" where 'name'='kunkun'
    """.stripMargin)
//  val sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"")
  println(sqlNode)
  println(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT))
  planner.validate(sqlNode)

  val relRoot = planner.rel(sqlNode)
  println(relRoot)
  println(s"${relRoot.kind}, ${relRoot.rel.getCorrelVariable}")

  println(RelOptUtil.toString(relRoot.project()))

  import scala.collection.JavaConversions._
  println(relRoot.project().getInput(0).getInput(0).asInstanceOf[LogicalFilter].getRowType)
  println(relRoot.project().getInput(0).getInput(0).asInstanceOf[LogicalFilter].getCondition)
  println(relRoot.project().getInput(0).getInput(0).asInstanceOf[LogicalFilter].getCondition.getKind)
  println(relRoot.project().getInputs.foreach(e => RelOptUtil.toString(e)))

  case class Triple(s: String, p: String, o: String)

  class TestSchema {
    var user: Array[Triple] = Array(Triple("1", "2", "3"))
  }
}

