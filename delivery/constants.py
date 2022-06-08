from schema import Schema


MESSAGE_SCHEMA = Schema({"fileURL": str,
"destination":str})

STRUCTURE_SOURCE={
    "client":{ 
        "delimiter": ";",
        "max_bad_rows" : 18,
        "encoding": "utf-8",
        "Schema" :[
        {
            "name": "NAME",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "AGE",
            "mode": "NULLABLE",
            "type": "INTEGER"
        }
    ]  
  },
  "telephone": { 
        "delimiter": ";",
        "max_bad_rows" : 18,
        "encoding": "utf-8",
        "Schema" :[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "number",
            "mode": "NULLABLE",
            "type": "INTEGER"
        }
    ]  
  },
  "address": { 
        "delimiter": ";",
        "max_bad_rows" : 18,
        "encoding": "utf-8",
        "Schema" :[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "address",
            "mode": "NULLABLE",
            "type": "STRING"
        }
    ]  
  }
}


TABLE_OUTPUT_SCHEMAS={
    "etienne12":[
        {
            "name": "NAME",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "AGE",
            "mode": "NULLABLE",
            "type": "INTEGER"
        }
    ],
    "etienne13":[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "number",
            "mode": "NULLABLE",
            "type": "INTEGER"
        }
    ],
    "etienne14":[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "address",
            "mode": "NULLABLE",
            "type": "STRING"
        }
    ]
}


REJECT_SCHEMA={
    "fields":[
        {
            "name": "timestamp",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "stacktrace",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "source_file",
            "mode": "NULLABLE",
            "type": "STRING"
        }]}


MONITORING_TABLE_SCHEMA={
    "fields":[
        {
            "name": "timestamp",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "number_inserted_rows",
            "mode": "NULLABLE",
            "type": "INTEGER"
        },
        {
            "name": "number_rejected_rows",
            "mode": "NULLABLE",
            "type": "INTEGER"
        }]}

dictTypes={
    'int64':'INTEGER',
    'object':'STRING',
    'float64':'FLOAT'
}