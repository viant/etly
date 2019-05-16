package etly

import (
	"fmt"
	"github.com/viant/dsc"
	"strings"
	"sync"
	"time"
	"github.com/viant/toolbox/url"
)

type VariableExtractions []*VariableExtraction

//Transfer represents a transfer rule
type Transfer struct {
	Name                 string
	Source               *Source
	Target               *Target
	Meta                 *Resource
	TimeWindow           *Duration
	Frequency            *Duration
	MaxParallelTransfers int
	MaxTransfers         int
	Transformer          string //name of registered transformer
	Filter               string //name of registered filter predicate
	VariableExtraction   VariableExtractions
	MaxErrorCounts       *int //maximum of errors that will stop process a file, if nil will process all file.
	nextRun              *time.Time
	running              bool
	lock                 sync.Mutex
	TimeOut              *Duration //Configured timeout for a transfer
	Repeat               int//in transferOnce number of execution
	ContentEnricher		 string
	FailRetry			 *int // number of times to retry if transfer fails
	UseFullData			 bool //log all the data 100% of the time - used during end-2-end tests
}

func (t *Transfer) Init() {
	if t.FailRetry == nil || *t.FailRetry < 1 {
		*t.FailRetry = 1
	}
}

func (t *Transfer) HasVariableExtraction() bool {
	return len(t.VariableExtraction) > 0
}

//HasRecordLevelVariableExtraction returns true if variable has record level rule
func (t *Transfer) HasRecordLevelVariableExtraction() bool {
	return t.VariableExtraction.HasRecordSource()
}

func (t *Transfer) Validate() error {
	_, hasProvider := NewProviderRegistry().registry[t.Source.DataType]
	if !hasProvider {
		return fmt.Errorf("failed to lookup provider for data type '%v':  %v -> %v", t.Source.DataType, t.Source.Name, t.Target.Name)
	}
	if t.Transformer != "" {
		_, hasTransformer := NewTransformerRegistry().registry[t.Transformer]
		if !hasTransformer {
			return fmt.Errorf("failed to lookup transormer for '%v':  %v -> %v", t.Transformer, t.Source.Name, t.Target.Name)
		}
	}
	return nil
}

//TransferConfig represents TransferConfig
type TransferConfig struct {
	Transfers []*Transfer
}

type Resource struct {
	Name           string
	Type           string //url,datastore
	DataFormat     string //nd_json,json
	Compression    string //gzip
	CredentialFile string
}

func (r *Resource) Clone() *Resource {
	if r == nil {
		return nil
	}
	return &Resource{
		Name:           r.Name,
		Type:           r.Type,
		DataFormat:     r.DataFormat,
		Compression:    r.Compression,
		CredentialFile: r.CredentialFile,
	}
}

type StructuredResource struct {
	*Resource
	DataType string //app data object name
	Schema   *Resource
	DsConfig *dsc.Config
}

func (r *StructuredResource) Clone() *StructuredResource {
	if r == nil {
		return nil
	}
	var result = &StructuredResource{
		Resource: r.Resource.Clone(),
		DataType: r.DataType,
		Schema:   r.Schema.Clone(),
		DsConfig: r.DsConfig,
	}
	return result
}

type Source struct {
	*StructuredResource
	FilterRegExp  string
	DataTypeMatch []*DataTypeMatch
	BatchSize     int //batch size for datastore source based transfer
}

func (r *Source) Clone() *Source {
	if r == nil {
		return nil
	}
	var result = &Source{
		StructuredResource: r.StructuredResource.Clone(),
		FilterRegExp:       r.FilterRegExp,
		DataTypeMatch:      r.DataTypeMatch,
	}
	return result
}

type Target struct {
	*StructuredResource
	TransferMethod string //upload only if url to datastore
	MaxAllowedSize int    //batch size for datastore source based transfer
}

func (r *Target) Clone() *Target {
	if r == nil {
		return nil
	}
	var result = &Target{
		StructuredResource: r.StructuredResource.Clone(),
		TransferMethod:     r.TransferMethod,
	}
	return result
}

type Duration struct {
	Duration int
	Unit     string
}

func (d *Duration) Clone() *Duration {
	if d == nil {
		return nil
	}
	var result = &Duration{
		Duration: d.Duration,
		Unit: d.Unit,
	}
	return  result
}

func (d *Duration) TimeUnit() (time.Duration, error) {
	switch strings.ToLower(d.Unit) {
	case "day":
		return 24 * time.Hour, nil
	case "hour":
		return time.Hour, nil
	case "min":
		return time.Minute, nil
	case "sec":
		return time.Second, nil
	case "milli":
		return time.Millisecond, nil
	}
	return 0, fmt.Errorf("Unsupported time unit %v", d.Unit)
}

func (d *Duration) Get() (time.Duration, error) {
	timeUnit, err := d.TimeUnit()
	if err != nil {
		return 0, err
	}
	return timeUnit * time.Duration(d.Duration), nil
}

func (t *Transfer) scheduleNextRun(now time.Time) error {
	delta, err := t.Frequency.Get()
	if err != nil {
		return err
	}
	nextRun := now.Add(delta)
	t.nextRun = &nextRun
	return nil
}

func (t *Transfer) isRunning() bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.running
}

func (t *Transfer) setRunning(running bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.running = running
}

func (t *Transfer) String() string {
	return "[id: " + t.Name + ", SourceURL: " + t.Source.Name + ", Target: " + t.Target.Name + "]"
}

func (t *Transfer) New(source, target, MetaURL string) *Transfer {
	var result = t.Clone()
	result.Source.Name = source
	result.Target.Name = target
	result.Meta.Name = MetaURL
	return result
}

//Clone creates a copy of the Transfer
func (t *Transfer) Clone() *Transfer {
	return &Transfer{
		Name:                 t.Name,
		Source:               t.Source.Clone(),
		Target:               t.Target.Clone(),
		Meta:                 t.Meta.Clone(),
		TimeWindow:           t.TimeWindow.Clone(),
		TimeOut:              t.TimeOut.Clone(),
		Frequency:            t.Frequency.Clone(),
		MaxParallelTransfers: t.MaxParallelTransfers,
		MaxTransfers:         t.MaxTransfers,
		Transformer:          t.Transformer,
		VariableExtraction:   t.VariableExtraction,
		Filter:               t.Filter,
		ContentEnricher:	  t.ContentEnricher,
		FailRetry:            t.FailRetry,
		UseFullData:          t.UseFullData,
	}
}

//DataTypeMatch represents a source data type matching rule,
type DataTypeMatch struct {
	MatchingFragment string
	DataType         string
}

//VariableExtraction represents variable extraction rule
type VariableExtraction struct {
	Name     string
	RegExpr  string
	Provider string //provider name for source or target record type only
	Source   string // sourceUrl, source, target (source or target refer to a data record)
}

func (e VariableExtractions) HasRecordSource() bool {
	for _, item := range e {
		if item.Source == "source" || item.Source == "target" {
			return true
		}
	}
	return false
}

// Host defines a host construct with IP/DNS and port
type Host struct {
	Server string
	Port   int
}

//ServerConfig ETL config
type ServerConfig struct {
	Production bool
	Port       int
	Cluster    []*Host
	TimeOut    *Duration //Configured timeout for a request from a server
}

//NewServerConfigFromURL creates a new config from URL
func NewServerConfigFromURL(URL string) (result *ServerConfig, err error) {
	result = &ServerConfig{}
	resource := url.NewResource(URL)
	return result, resource.JSONDecode(result)
}

//NewTransferConfigFromURL creates a new config from URL
func NewTransferConfigFromURL(URL string) (result *TransferConfig, err error) {
	result = &TransferConfig{}
	resource := url.NewResource(URL)
	return result, resource.JSONDecode(result)
}
