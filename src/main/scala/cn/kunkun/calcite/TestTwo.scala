package cn.kunkun.calcite

import org.apache.calcite.adapter.java.ReflectiveSchema
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.Frameworks

object TestTwo extends App {

  val schemaPlus = Frameworks.createRootSchema(true)
  schemaPlus.add("T", new ReflectiveSchema(new TestSchema))
  val frameworkConfig = Frameworks.newConfigBuilder()
    .defaultSchema(schemaPlus)
    .build()
  val parserConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig)
  parserConfig.setCaseSensitive(false).setConfig(parserConfig.build())
  val planner = Frameworks.getPlanner(frameworkConfig)
  val sqlNode = planner.parse(
    """
      |select * from "T"."user"
    """.stripMargin)
//  val sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"")

  planner.validate(sqlNode)
  val relRoot = planner.rel(sqlNode)
  println(RelOptUtil.toString(relRoot.project()))

  case class Triple(s: String, p: String, o: String)

  class TestSchema {
    var user: Array[Triple] = Array(Triple("1", "2", "3"))
  }
}

