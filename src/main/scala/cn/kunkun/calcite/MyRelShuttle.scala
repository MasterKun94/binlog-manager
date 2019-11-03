package cn.kunkun.calcite

import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttle}

import scala.collection.mutable

class MyRelShuttle extends RelShuttle {
  private val stack: mutable.Stack[MyNode] = mutable.Stack()

  override def visit(scan: TableScan): RelNode = ???

  override def visit(scan: TableFunctionScan): RelNode = ???

  override def visit(values: LogicalValues): RelNode = ???

  override def visit(filter: LogicalFilter): RelNode = ???

  override def visit(project: LogicalProject): RelNode = {
    project.getChildExps.get(0).toString
    ???
  }

  override def visit(join: LogicalJoin): RelNode = ???

  override def visit(correlate: LogicalCorrelate): RelNode = ???

  override def visit(union: LogicalUnion): RelNode = ???

  override def visit(intersect: LogicalIntersect): RelNode = ???

  override def visit(minus: LogicalMinus): RelNode = ???

  override def visit(aggregate: LogicalAggregate): RelNode = ???

  override def visit(`match`: LogicalMatch): RelNode = ???

  override def visit(sort: LogicalSort): RelNode = ???

  override def visit(exchange: LogicalExchange): RelNode = ???

  override def visit(other: RelNode): RelNode = ???
}
