package etly

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/viant/toolbox"
)

type VariableExtractions []*VariableExtraction

//ProcessedTransfers represents ProcessedTransfers rule
type Transfer struct {
	Name string

	Source *Source
	Target *Target
	Meta   *Resource

	TimeWindow *Duration
	Frequency  *Duration

	MaxParallelTransfers int
	MaxTransfers         int

	Transformer        string //name of registered transformer
	Filter             string //name of registered filter predicate
	VariableExtraction []*VariableExtraction

	nextRun *time.Time
	running bool

	lock sync.Mutex
}

func (t *Transfer) HasVariableExtraction() bool {
	return len(t.VariableExtraction) > 0
}

//HasRecordLevelVariableExtraction returns true if variable has record level rule
func (t *Transfer) HasRecordLevelVariableExtraction() bool {
	var variableExtractions VariableExtractions = t.VariableExtraction
	return variableExtractions.HasRecordSource()
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
	Encoding       string
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
		Encoding:       r.Encoding,
		CredentialFile: r.CredentialFile,
	}
}

type StructuredResource struct {
	*Resource
	DataType string //app data object name
	Schema   *Resource
}

func (r *StructuredResource) Clone() *StructuredResource {
	if r == nil {
		return nil
	}
	var result = &StructuredResource{
		Resource: r.Resource.Clone(),
		DataType: r.DataType,
		Schema:   r.Schema.Clone(),
	}
	return result
}

type Source struct {
	*StructuredResource
	FilterRegExp  string
	DataTypeMatch []*DataTypeMatch
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

//Clone creates a copy of the ProcessedTransfers
func (t *Transfer) Clone() *Transfer {
	return &Transfer{
		Name:                 t.Name,
		Source:               t.Source.Clone(),
		Target:               t.Target.Clone(),
		Meta:                 t.Meta.Clone(),
		TimeWindow:           t.TimeWindow,
		Frequency:            t.Frequency,
		MaxParallelTransfers: t.MaxParallelTransfers,
		MaxTransfers:         t.MaxTransfers,
		Transformer:          t.Transformer,
		VariableExtraction:   t.VariableExtraction,
		Filter:               t.Filter,
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
	Port    int
	Cluster []*Host
}

//NewServerConfigFromURL creates a new config from URL
func NewServerConfigFromURL(URL string) (result *ServerConfig, err error) {
	result = &ServerConfig{}
	err = toolbox.LoadConfigFromUrl(URL, result)
	return
}

//NewTransferConfigFromURL creates a new config from URL
func NewTransferConfigFromURL(URL string) (result *TransferConfig, err error) {
	result = &TransferConfig{}
	err = toolbox.LoadConfigFromUrl(URL, result)
	return
}
