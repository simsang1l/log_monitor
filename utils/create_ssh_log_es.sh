curl -X PUT "localhost:9200/ssh-log" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "date":       { "type": "date" },
      "event_time": { "type": "date" },
      "row_hash":   { "type": "keyword" },
      "msg":        { "type": "text" },
      "source_path":{ "type": "keyword" },
      "source_file":{ "type": "keyword" }
    }
  }
}'