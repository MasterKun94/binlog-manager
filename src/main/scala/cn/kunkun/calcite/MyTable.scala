package cn.kunkun.calcite

import java.util

import cn.kunkun.binlog.BTable
import cn.kunkun.binlog.Statistics.{ForeignKey, PrimaryKey, SortIndex, UniqueKey}
import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.{RelCollation, RelCollationImpl, RelReferentialConstraint, RelReferentialConstraintImpl}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.{Schema, Statistic, Statistics}
import org.apache.calcite.sql.{SqlCall, SqlNode}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.mapping.IntPair

import scala.collection.JavaConversions._

class MyTable(table: BTable) extends AbstractTable {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    typeFactory.createStructType(new util.ArrayList(table.getColumns
      .map(col => (col.getName.name, typeFactory.createSqlType(TypeUtil.getRelDataType(col.getType))))
      .toMap
      .entrySet())
    )
  }

  override def getJdbcTableType: Schema.TableType = super.getJdbcTableType

  override def getStatistic: Statistic = {

    val bitset = Seq.newBuilder[ImmutableBitSet]
    val relref = Seq.newBuilder[RelReferentialConstraint]
    val relCol = Seq.newBuilder[RelCollation]
    val statistics = table.getStatistics
    for (idx <- statistics.indices) {
      statistics(idx) match {
        case key: PrimaryKey => //TODO
          bitset += ImmutableBitSet.of(asJavaIterable(key.getColIndices.map(Integer.valueOf)))

        case key: UniqueKey =>
          bitset += ImmutableBitSet.of(asJavaIterable(key.getColIndices.map(Integer.valueOf)))

        case key: ForeignKey =>
          relref += RelReferentialConstraintImpl.of(
            Seq(table.tableName.name),
            Seq(key.foreign.getTable.tableName.name),
            key.getPairIndices.map(pair => IntPair.of(pair._1, pair._2)))
        case key: SortIndex =>
          relCol += RelCollationImpl.of() //TODO
      }
    }
    Statistics.of(null, bitset.result(), relref.result(), relCol.result()) //TODO
  }

  override def isRolledUp(column: String): Boolean = super.isRolledUp(column)

  override def rolledUpColumnValidInsideAgg(column: String, call: SqlCall, parent: SqlNode,
                                            config: CalciteConnectionConfig): Boolean = {
    super.rolledUpColumnValidInsideAgg(column, call, parent, config)
  }

  override def unwrap[C](aClass: Class[C]): C = super.unwrap(aClass)
}
