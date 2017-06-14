# Etl etly - Basic data transformation framework for go


This library is compatible with Go 1.5+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#Motivation)
- [Collection Utilities](#Collection-Utilities)
- [Getting Started](#Getting-Started)



## Motivation
<a name="Motivation"></a>

This library was developed as part to simplify data transformation.
It uses generic concept of source and target, which could be some remote storage
or in the feature datastore tables.

<a name="Getting-Started"></a>
## Getting Started
### Multi Storage File ETL Setup

- Create source message
- Create target message
- Create transformer 

Example
```go
var SourceLogTransformer = func(source interface{}) (interface{}, error) {
	sourceLog, casted := source.(*SourceLog)
	if ! casted {
		return nil, fmt.Errorf("Failed to cast source: %T, expected %T", source, &SourceLog{})
	}
	var targetLog = &TargetLog{}
	//... transformation comes here
	return targetLog , nil
}

```
- Create source message provider

Example
```go
var SourceLogProvider =  = func() interface{} {
		return &SourceLog{}
	}

```

 - Create optionally source log filter

Example

```go

type SourceLogFilter struct {}


func (f *SourceLogFilter) etlyly(source interface{}) bool {
    sourceLog, casted := source.(*SourceLog)
	if ! casted {
		return nil, fmt.Errorf("Failed to cast source: %T, expected %T", source, &SourceLog{})
	}
	//filter only click type log
	if sourceLog.Type == "click"  {
	    return true
	}
	return false
}

```

 - Create register all components in the init
 
 ```go

    func init() {
    	etly.NewTransformerRegistry().Register("SourceLogTransformer", SourceLogTransformer)
    	etly.NewProviderRegistry().Register("SourceLog", SourceLogProvider)
    	etly.NewFilterRegistry().Register("SourceLogFilter", &SourceLogFilter{})
      
    }


 ```
 
 - Create config
 
 Example
```json
{
  "Transfers": [
    {
      "Name":"S3 Bid Logs To GS",
      "Source": "s3://bucket//<dateFormat:yyyy>/<dateFormat:MM>/<dateFormat:dd>/<dateFormat:hh>/",
      "SourceType": "url",
      "SourceFormat": "ndjson",
      "SourceEncoding": "gzip",
      "SourceDataType": "SourceLog",
      "SourceDataTypeMatch": [{
        "MatchingFragment": "\"exclude\""
      }],
      "Target": "gs://bucket/table/<dateFormat:yyyy><dateFormat:MM><dateFormat:dd>/<mod:40>/<dateFormat:hh>-<file>",
      "TargetType": "url",
      "TargetEncoding": "gzip",
      "TimeWindow": 48,
      "TimeWindowUnit": "hour",
      "MetaUrl": "gs://bucket/table/<dateFormat:yyyy><dateFormat:MM><dateFormat:dd>/transfer-meta.json",
      "Filter": "SourceLogFilter",
      "Transformer": "SourceLogTransformer",
      "MaxParallelTransfers": 4,
      "MaxTransfers": 8,
      "TimeFrequency": 1,
      "TimeFrequencyUnit": "min"
    }
  ],
  "Port":8081,

  "Storage": [
    {
      "Schema": "gs",
      "config": "/secret/gs.json"
    },
    {
      "Schema": "s3",
      "config": "/secret/s3.json"
    }
  ]
}
```



 -  Add dep to the main func to make sure that init runs
 
 ```go
import (
	_ "your/dep/path"
)
```

 
 - Optionally assembly your main func
 
 ```go
var (
	configUrl = flag.String("configUrl", "", "")
)

func main() {
	flag.Parse()
	config, err := etly.NewConfigFromURL(*configUrl)
	if err != nil {
		log.Fatal(err)
	}
	service, err := etly.NewServer(config)
	if err != nil {
		log.Fatal(err)
	}
	service.Start();

}

```

  - run service

  - check service status 
    
        http://127.0.0.1:8081/etly/status
        
        http://127.0.0.1:8081/etly/tasks/
       
        
 