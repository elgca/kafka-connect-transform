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
  * 我更希望在flattening后的数据提供op和ts_ms字段
  * 'op'在抽取后字段名默认为'__operated'
  * 'ts_ms'抽取后默认字段名为'__time'
  *
  * @tparam R <R> the subtype of { @link ConnectRecord} on which this transformation will operate
  */
class UnwrapDebeziumCDC[R <: ConnectRecord[R]] extends Transformation[R] {
  private final val logger = LoggerFactory.getLogger(getClass)
  private final val ENVELOPE_SCHEMA_NAME_SUFFIX = ".Envelope"

  private val afterDelegate = new ExtractField.Value[R]
  private val beforeDelegate = new ExtractField.Value[R]

  final val CASE_CONVERSION = "case.conversion"
  final val CASE_CONVERSION_DEFAULT = "lower"

  final val BEFORE_FIELD = "before.field"
  final val BEFORE_FIELD_DEFAULT = "before"
  final val AFTER_FIELD = "after.field"
  final val AFTER_FIELD_DEFAULT = "after"
  final val TS_MS_NAME_FIELD = "ts_ms.field"
  final val TS_MS_NAME_FIELD_DEFAULT = "ts_ms"
  final val OPERATE_FIELD = "op.field"
  final val OPERATE_FIELD_DEFAULT = "op"

  final val TS_MS_NAME = "ts_ms.name"
  final val TS_MS_NAME_DEFAULT = "__time"
  final val OPERATE_NAME = "op.name"
  final val OPERATE_NAME_DEFAULT = "__operate"

  private var before_field: String = _
  private var after_field: String = _
  private var operate_field: String = _
  private var ts_ms_field: String = _
  private var opName: String = _
  private var timeName: String = _
  private var conversion: String = _

  private var schemaUpdateCache: util.HashMap[Schema, Schema] = _

  def converter(name: String): String = {
    conversion match {
      case "lower" => name.toLowerCase
      case "upper" => name.toUpperCase
      case _ => name
    }
  }

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
      val opSchema = baseStruct.schema().field(operate_field).schema()
      val ts_msSchema = baseStruct.schema().field(ts_ms_field).schema()
      updatedSchema = appendSchema(value.schema,
        (opName, opSchema),
        (timeName, ts_msSchema))
      schemaUpdateCache.put(baseStruct.schema, updatedSchema)
    }

    val op = baseStruct.get(operate_field)
    val ts_ms = baseStruct.get(ts_ms_field)

    val updatedValue = new Struct(updatedSchema)
    for (field <- value.schema.fields.asScala) {
      updatedValue.put(converter(field.name), value.get(field))
    }
    updatedValue.put(converter(opName), op)
    updatedValue.put(converter(timeName), ts_ms)

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
      builder.field(converter(field.name), field.schema)
    }
    for (field <- fields) {
      builder.field(converter(field._1), field._2)
    }
    builder.build()
  }


  override def config(): ConfigDef = {
    new ConfigDef()
      .define(BEFORE_FIELD,
        ConfigDef.Type.STRING,
        BEFORE_FIELD_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        """before field""")
      .define(AFTER_FIELD,
        ConfigDef.Type.STRING,
        AFTER_FIELD_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        """after field""")
      .define(TS_MS_NAME_FIELD,
        ConfigDef.Type.STRING,
        TS_MS_NAME_FIELD_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        """时间字段,ts_ms""")
      .define(TS_MS_NAME,
        ConfigDef.Type.STRING,
        TS_MS_NAME_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        """时间字段输出名称,默认__time""")
      .define(OPERATE_FIELD,
        ConfigDef.Type.STRING,
        OPERATE_FIELD_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        """操作字段,默认op""")
      .define(OPERATE_NAME,
        ConfigDef.Type.STRING,
        OPERATE_NAME_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        """操作字段输出名称,默认 __operate""")


  }

  override def close(): Unit = {
    beforeDelegate.close()
    afterDelegate.close()
    schemaUpdateCache = null
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    opName = Option(configs.get(OPERATE_NAME).asInstanceOf[String]).getOrElse(OPERATE_NAME_DEFAULT)
    timeName = Option(configs.get(TS_MS_NAME).asInstanceOf[String]).getOrElse(TS_MS_NAME_DEFAULT)
    before_field = Option(configs.get(BEFORE_FIELD)).map(_.toString).getOrElse(BEFORE_FIELD_DEFAULT)
    after_field = Option(configs.get(AFTER_FIELD)).map(_.toString).getOrElse(AFTER_FIELD_DEFAULT)
    operate_field = Option(configs.get(OPERATE_FIELD)).map(_.toString).getOrElse(OPERATE_FIELD_DEFAULT)
    ts_ms_field = Option(configs.get(TS_MS_NAME_FIELD)).map(_.toString).getOrElse(TS_MS_NAME_FIELD_DEFAULT)
    conversion = Option(configs.get(CASE_CONVERSION)).map(_.toString).getOrElse(CASE_CONVERSION_DEFAULT)
    //init schema cache
    schemaUpdateCache = new util.HashMap[Schema, Schema]

    val delegateConfig = new util.HashMap[String, String]()
    delegateConfig.put("field", before_field)
    beforeDelegate.configure(delegateConfig)

    val delegateConfig2 = new util.HashMap[String, String]()
    delegateConfig2.put("field", after_field)
    afterDelegate.configure(delegateConfig2)
  }
}
