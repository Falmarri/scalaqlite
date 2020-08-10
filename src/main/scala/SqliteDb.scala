// Copyright (c) 2010 Sean C. Rhea <sean.c.rhea@gmail.com>
// All rights reserved.
//
// See the file LICENSE included in this distribution for details.

package org.srhea.scalaqlite

import java.sql.{DriverManager, PreparedStatement, ResultSet, Statement, Types}

import org.sqlite.{SQLiteConfig, SQLiteConnection, SQLiteException}

case class SqlException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)

object SqlValue {
  val charset = java.nio.charset.Charset.forName("UTF-8")
}

abstract class SqlValue {
  def toDouble: Double = throw SqlException(s"$this is not a double")
  def toInt: Int = throw SqlException(s"$this is not an int")
  def toLong: Long = throw SqlException(s"$this is not an long")
  def toBlob: Seq[Byte] = throw SqlException(s"$this is not a blob")
  def isNull = false
  def bindValue(stmt: PreparedStatement, col: Int): Unit
}
case object SqlNull extends SqlValue {
  override def toString = "NULL"
  override val isNull = true
  override def bindValue(stmt: PreparedStatement, col: Int) = stmt.setNull(col, java.sql.Types.NULL )
}
case class SqlLong(i: Long) extends SqlValue {
  override def toString = i.toString
  override def toDouble = i
  override def toInt =
    if (i <= Integer.MAX_VALUE && i >= Integer.MIN_VALUE) i.toInt else super.toInt
  override def toLong = i
  override def bindValue(stmt: PreparedStatement, col: Int) = stmt.setLong(col, i)
}
case class SqlDouble(d: Double) extends SqlValue {
  override def toString = d.toString
  override def toDouble = d
  override def toInt = if (d.round == d) d.toInt else super.toInt
  override def toLong = if (d.round == d) d.toLong else super.toLong
  override def bindValue(stmt: PreparedStatement, col: Int) = stmt.setDouble(col, d)
}
case class SqlBlob(bytes: Seq[Byte]) extends SqlValue {
    override def toBlob = bytes
    override def toString = new String(bytes.toArray)
    override def bindValue(stmt: PreparedStatement, col: Int) = stmt.setBytes(col, bytes.toArray)
}
case class SqlText(s: String) extends SqlValue {
    override def toString = s
    override def bindValue(stmt: PreparedStatement, col: Int) = stmt.setString(col, s)
}

class SqliteStatement(private val stmt: PreparedStatement) {
    def query[R](params: SqlValue*)(f: Iterator[IndexedSeq[SqlValue]] => R): R = {
        params.foldLeft(1) { (i, param) => param.bindValue(stmt, i); i + 1 }
        stmt.execute()
        val resultSet = stmt.getResultSet()
        val it = new Iterator[IndexedSeq[SqlValue]] {
            private var available = resultSet != null && resultSet.next()
            override def hasNext: Boolean = available
            override def next(): IndexedSeq[SqlValue] = {
                val meta = stmt.getMetaData
                val rs = (1 to meta.getColumnCount).map{ i=>
                    resultSet.getObject(i) match {
                      case null => SqlNull
                      case i: java.lang.Long => SqlLong(i)
                      case i: java.lang.Integer => SqlLong(i.toLong)
                      case i: java.lang.Double => SqlDouble(i)
                      case s: String => SqlText(s)
                      case a: Array[Byte] => SqlBlob(a)
                      case other => throw SqlException(s"unsupported type: ${other}")
                    }
                }
                available = resultSet.next()
                rs

            }
        }

        try f(it) finally stmt.clearParameters()
    }
    def foreachRow(params: SqlValue*)(f: IndexedSeq[SqlValue] => Unit) {
      query(params:_*) { i => i.foreach { row => f(row) } }
    }
    def execute(params: SqlValue*): Unit = { query(params:_*) { i => i.foreach { _ => () } } }
    def mapRows[R](params: SqlValue*)(f: IndexedSeq[SqlValue] => R) = query(params:_*)(_.map(f).toSeq)
    def getRows(params: SqlValue*) = query(params:_*)(_.toSeq)
    def close: Unit = ()
}


class SqliteDb(path: String) {

    private val db = DriverManager.getConnection(s"jdbc:sqlite:${path}")
    def close() {
        db.close()
    }
    def prepare[R](sql: String)(f: SqliteStatement => R): R = {
        assert(!db.isClosed, "db is closed")
      try{
        val statement: PreparedStatement = db.prepareStatement(sql)
        val stmt = new SqliteStatement(statement)
          f(stmt)
        } catch {
          case e: SQLiteException => throw SqlException(e.getMessage, e)
        }
    }
    def query[R](sql: String, params: SqlValue*)(f: Iterator[IndexedSeq[SqlValue]] => R): R =
      prepare(sql)(_.query(params:_*)(f))
    def foreachRow(sql: String, params: SqlValue*)(f: IndexedSeq[SqlValue] => Unit) =
      prepare(sql)( _.foreachRow(params:_*)(f))
    def mapRows[R](sql: String, params: SqlValue*)(f: IndexedSeq[SqlValue] => R) =
      prepare(sql)(_.mapRows(params:_*)(f))
    def getRows(sql: String, params: SqlValue*) = prepare(sql)(_.getRows(params:_*))
    def execute(sql: String, params: SqlValue*): Unit = { prepare(sql)(_.execute(params:_*)) }
    def enableLoadExtension(on: Boolean) = {
      // This cast is ugly. But changing this value on an existing connection isn't supported
      // from the JDBC api. We can set this on new connections via SQLiteConfig, but in order
      // to change it directly we have to access the native SQL db.
      // See https://github.com/xerial/sqlite-jdbc/pull/319 for a potentially better way of
      // doing this
      db.asInstanceOf[SQLiteConnection].getDatabase.enable_load_extension(on)
    }
    def errmsg: String = ""
    def changeCount: Int = {
      // This can be done with `select changes()`, but this is probably faster, and since we
      // already need to cast this above for enableLoadExtension, might as well do it here
      // too
      db.asInstanceOf[SQLiteConnection].getDatabase.changes()
    }
}
