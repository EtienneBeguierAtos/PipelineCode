table_schema = {
    'fields':[
        {
            "name": "name",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "age",
            "mode": "NULLABLE",
            "type": "INTEGER"
        },
        {
            "name": "height",
            "mode": "NULLABLE",
            "type": "FLOAT"
        }
    ] 
}


table_schema2 = {
    'fields':[
        {
            "name": "NAME",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "AGE",
            "mode": "NULLABLE",
            "type": "INTEGER"
        },
        {
            "name": "COLOR",
            "mode": "NULLABLE",
            "type": "STRING"
        }
    ] 
}


tables={
    "tabledata.csv": table_schema,
    "tabledata2.csv":table_schema2
}