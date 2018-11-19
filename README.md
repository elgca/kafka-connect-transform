# kafka-connect-transform

## UnwrapDebeziumCDC

对Debezium的CDC数据进行flatten转换

- io.kafka.connect.UnwrapDebeziumCDC
- 对于insert/update数据保留after字段，并添加op和ts_ms
- 对于delete,保留原始数据,并添加op和ts_ms

```json
{
	"op": "u",
	"source": {
		...
	},
	"ts_ms" : ...,
	"before" : {
		"field1" : "oldvalue1",
		"field2" : "oldvalue2"
	},
	"after" : {
		"field1" : "newvalue1",
		"field2" : "newvalue2"
	}
}
```

```json
{
	"field1" : "newvalue1",
	"field2" : "newvalue2",
  "__operate" : "u"(op),
  "__time" : ...(ts_ms)
}
```
