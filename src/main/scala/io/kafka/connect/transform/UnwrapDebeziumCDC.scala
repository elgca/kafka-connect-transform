package io.kafka.connect.transform

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Field, Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.transforms.{ExtractField, InsertField, Transformation}
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
  * Debezium 的UnwrapFromEnvelope提供了对CDC数据的平展，并添加删除标记；
  * 我更希望在flattening后的数据提供op和tm_ms字段
  * 'op'在抽取后字段名默认为'__operated'
  * 'tm_ms'抽取后默认字段名为'__time'
  *
  * @tparam R <R> the subtype of { @link ConnectRecord} on which this transformation will operate
  */
class UnwrapDebeziumCDC[R <: ConnectRecord[R]] extends Transformation[R] {
  private final val logger = LoggerFactory.getLogger(getClass)
  private final val ENVELOPE_SCHEMA_NAME_SUFFIX = ".Envelope"

  private val afterDelegate = new ExtractField.Value[R]
  private val beforeDelegate = new ExtractField.Value[R]

  val TM_MS_NAME = "tm_ms.name"
  val TM_MS_NAME_DEFAULT = "__time"
  val OPERATE_NAME = "op.name"
  val OPERATE_NAME_DEFAULT = "__operate"

  private var opName: String = _
  private var timeName: String = _

  private var schemaUpdateCache: util.HashMap[Schema, Schema] = _

  override def apply(record: R): R = {
    if (record.value == null) {
      return record
    }
    if (record.valueSchema == null ||
      record.valueSchema.name == null ||
      !record.valueSchema.name.endsWith(ENVELOPE_SCHEMA_NAME_SUFFIX)) {
      logger.warn("Expected Envelope for transformation, passing it unchanged")
      return record
    }
    val newRecord = afterDelegate(record)
    if (newRecord.value == null) {
      logger.trace("Delete message {} requested to be rewritten", record.key)
      val oldRecord = beforeDelegate(record)
      updatedDelegate(oldRecord, record)
    } else {
      updatedDelegate(newRecord, record)
    }
  }

  def updatedDelegate(record: R, base: R): R = {

    val value = requireStruct(record.value(), this.getClass.getName)

    val baseStruct = requireStruct(base.value(), this.getClass.getName)

    var updatedSchema = schemaUpdateCache.get(value.schema)
    if (updatedSchema == null) {
      val opSchema = baseStruct.schema().field("op").schema()
      val ts_msSchema = baseStruct.schema().field("ts_ms").schema()
      updatedSchema = appendSchema(value.schema,
        (opName, opSchema),
        (timeName, ts_msSchema))
      schemaUpdateCache.put(baseStruct.schema, updatedSchema)
    }

    val op = baseStruct.get("op")
    val ts_ms = baseStruct.get("ts_ms")

    val updatedValue = new Struct(updatedSchema)
    for (field <- value.schema.fields.asScala) {
      updatedValue.put(field.name, value.get(field))
    }
    updatedValue.put(opName, op)
    updatedValue.put(timeName, ts_ms)

    record.newRecord(record.topic,
      record.kafkaPartition,
      record.keySchema,
      record.key,
      updatedSchema,
      updatedValue,
      record.timestamp)

  }

  private def appendSchema(schema: Schema, fields: (String, Schema)*): Schema = {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)
    for (field <- schema.fields().asScala) {
      builder.field(field.name, field.schema)
    }
    for (field <- fields) {
      builder.field(field._1, field._2)
    }
    builder.build()
  }


  override def config(): ConfigDef = {
    new ConfigDef()
      .define(TM_MS_NAME,
        ConfigDef.Type.STRING,
        TM_MS_NAME_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        """tm_ms 字段映射名称""")
      .define(OPERATE_NAME,
        ConfigDef.Type.STRING,
        OPERATE_NAME_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        """op 字段映射名称""")
  }

  override def close(): Unit = {
    beforeDelegate.close()
    afterDelegate.close()
    schemaUpdateCache = null
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    opName = Option(configs.get(OPERATE_NAME).asInstanceOf[String]).getOrElse(OPERATE_NAME_DEFAULT)
    timeName = Option(configs.get(TM_MS_NAME).asInstanceOf[String]).getOrElse(TM_MS_NAME_DEFAULT)
    //init schema cache
    schemaUpdateCache = new util.HashMap[Schema, Schema]

    val delegateConfig = new util.HashMap[String, String]()
    delegateConfig.put("field", "before")
    beforeDelegate.configure(delegateConfig)

    val delegateConfig2 = new util.HashMap[String, String]()
    delegateConfig2.put("field", "after")
    afterDelegate.configure(delegateConfig2)


  }
}
