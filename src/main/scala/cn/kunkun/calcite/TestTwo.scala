package cn.kunkun.calcite

import cn.kunkun.add.BColumn.Type
import cn.kunkun.add.ImplicitUtil.str2Symbol
import cn.kunkun.add.BStatistics.PrimaryKey
import cn.kunkun.add.{BColumn, BSchema, BSchemaRegistry, BTable}
import cn.kunkun.calcite.ImplicitUtil._
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.sql.dialect.CalciteSqlDialect
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.Frameworks

object TestTwo extends App {

  implicit val registry: BSchemaRegistry = BSchemaRegistry.DEFAULT

  BSchema.of("test")
  BTable.of("test", "user")(
    BColumn("id", Type.LONG),
    BColumn("name", Type.VARCHAR),
    BColumn("age", Type.LONG),
    BColumn("hobby", Type.VARCHAR)
  )(
    PrimaryKey("id")
  )
  println(registry)
  println(registry.getSchema('test).get.tableNames)

  val schemaPlus = Frameworks.createRootSchema(true)
  schemaPlus.registryFrom(registry)
  val frameworkConfig = Frameworks.newConfigBuilder()
    .defaultSchema(schemaPlus)
    .build()
  println(schemaPlus.getSubSchema("test"))

  val parserConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig)
  parserConfig.setCaseSensitive(false).setConfig(parserConfig.build())

  val sqls = Array(
//    """
//      |delete from "test"."user" where "name"='kunkun'
//    """.stripMargin,
    """
      |select "id", count("age") from "test"."user" group by "name", "id"
    """.stripMargin
//    """
//      |update "test"."user" set "id" = 2 where "name"='kunkun'
//    """.stripMargin,
//    """
//      |insert into "test"."user" ("name", "id", "age", "hobby") values ('lala', 12, 42, 'hoho')
//    """.stripMargin
  )
  for (elem <- sqls) {
    val planner = Frameworks.getPlanner(frameworkConfig)
    val sqlNode = planner.parse(elem)
    println(sqlNode)
    println(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT))
    planner.validate(sqlNode)
    val relRoot = planner.rel(sqlNode)
    println(relRoot)
    println(s"${relRoot.kind}, ${relRoot.rel.getCorrelVariable}")
    println(relRoot.project().getInput(0).asInstanceOf[LogicalAggregate].getAggCallList.get(0))

    println(RelOptUtil.toString(relRoot.project()))

    println("--------------------------")
  }




}

