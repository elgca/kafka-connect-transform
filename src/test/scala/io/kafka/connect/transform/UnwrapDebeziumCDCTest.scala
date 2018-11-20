package io.kafka.connect.transform

import java.io.Closeable
import java.util

import io.debezium.data.Envelope
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.FunSuite

class UnwrapDebeziumCDCTest extends FunSuite {
  val TM_MS_NAME = "tm_ms.name"
  val TM_MS_NAME_DEFAULT = "__time"
  val OPERATE_NAME = "op.name"
  val OPERATE_NAME_DEFAULT = "__operate"


  def use[T <: Closeable, U](t: T)(f: T => U): U = {
    try f(t)
    finally t.close()
  }


  private def createDeleteRecord() = {
    val recordSchema = SchemaBuilder.struct.field("id", SchemaBuilder.int8).build
    val envelope = Envelope
      .defineSchema
      .withName("dummy.Envelope")
      .withRecord(recordSchema)
      .withSource(SchemaBuilder.struct.build)
      .build
    val before = new Struct(recordSchema)
    before.put("id", 1.toByte)
    val payload = envelope.delete(before, null, System.nanoTime)
    new SourceRecord(new util.HashMap[String, AnyRef](), new util.HashMap[String, AnyRef](),
      "dummy",
      envelope.schema,
      payload)
  }


  private def createCreateRecord() = {
    val recordSchema = SchemaBuilder.struct.field("id", SchemaBuilder.int8).build
    val envelope = Envelope
      .defineSchema
      .withName("dummy.Envelope")
      .withRecord(recordSchema)
      .withSource(SchemaBuilder.struct.build)
      .build
    val before = new Struct(recordSchema)
    before.put("id", 1.toByte)
    val payload = envelope.create(before, null, System.nanoTime)
    new SourceRecord(new util.HashMap[String, AnyRef](), new util.HashMap[String, AnyRef](),
      "dummy", envelope.schema, payload)
  }

  test("test delete record") {
    use(new UnwrapDebeziumCDC[SourceRecord]) {
      transform =>
        val props = new util.HashMap[String, String]()
        transform.configure(props)
        val record = createDeleteRecord()
        val res = transform.apply(record)
        assert(res.value().asInstanceOf[Struct].getString("__operate") == "d")
    }
  }

  test("test create record") {
    use(new UnwrapDebeziumCDC[SourceRecord]) {
      transform =>
        val props = new util.HashMap[String, String]()
        transform.configure(props)
        val record = createCreateRecord()
        val res = transform.apply(record)
        assert(res.value().asInstanceOf[Struct].getString("__operate") == "c")

    }
  }

  test("test case conversion") {
    use(new UnwrapDebeziumCDC[SourceRecord]) {
      transform =>
        val props = new util.HashMap[String, String]()
        props.put("case.conversion","upper")
        transform.configure(props)
        val record = createCreateRecord()
        val res = transform.apply(record)
        assert(res.value().asInstanceOf[Struct].getString("__OPERATE") == "c")

    }
  }

//  test("testConfig") {
//
//  }

}
