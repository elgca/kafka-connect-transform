package io.kafka.connect.transform

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.transforms.Transformation

class JsonTransform[R <: ConnectRecord[R]] extends Transformation[R] {
  private var cacheSchema: util.HashMap[Schema, Schema] = _

  override def apply(record: R): R = ???

  override def config(): ConfigDef = ???

  override def close(): Unit = ???

  override def configure(configs: util.Map[String, _]): Unit = ???
}
