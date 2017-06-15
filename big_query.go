package etly



import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"context"
	"google.golang.org/api/option"
	"strconv"
	"time"
	"strings"
)

type BigqueryService interface {
	// Perform a load job. This method performs blocking operations so it is ideal
	// to be executed in a go routine.
	Load(loadJob *LoadJob) (*bigquery.JobStatus, string, error)
}

type gbqService struct{}

// LoadJob contains all necessary information for GBQ Service to perform its task
type LoadJob struct {
	Credential string
	TableId    string
	DatasetId  string
	ProjectId  string
	Schema     bigquery.Schema
	URIs       []string
}

const (
	// e.g key1--val1__key2--val2__key3--val3__.....
	KeyValueSeparator = "--"
	PairSeparator     = "__"

	ErrorDuplicate    = "Error 409"
)

func NewBigqueryService() BigqueryService {
	return &gbqService{}
}

func (sv *gbqService) Load(loadJob *LoadJob) (*bigquery.JobStatus, string, error) {
	clientOption := option.WithServiceAccountFile(loadJob.Credential)
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, loadJob.ProjectId, clientOption)
	if err != nil {
		return nil, "", err
	}
	defer client.Close()
	jobId := sv.generateJobId(
		"ProjectId", loadJob.ProjectId,
		"DatasetId", loadJob.DatasetId,
		"TableId", loadJob.TableId,
		"Ts", strconv.FormatInt(time.Now().Unix(), 10))
	ref := bigquery.NewGCSReference(loadJob.URIs...)
	if loadJob.Schema == nil {
		ref.AutoDetect = true
	} else {
		ref.Schema = loadJob.Schema
	}
	ref.SourceFormat = bigquery.JSON
	dataset := client.DatasetInProject(loadJob.ProjectId, loadJob.DatasetId)
	if err := dataset.Create(ctx); err != nil {
		// Create dataset if it does exist, otherwise ignore duplicate error
		if !strings.Contains(err.Error(), ErrorDuplicate) {
			return nil, "", err
		}
	}
	loader := dataset.Table(loadJob.TableId).LoaderFrom(ref)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	loader.WriteDisposition = bigquery.WriteAppend
	loader.JobID = jobId
	job, err := loader.Run(ctx)
	if err != nil {
		return nil, "", err
	}
	status, err := job.Wait(ctx)
	return status, jobId, err
}

// Generate job ID following best practices:
// https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects#generate-jobid
// This method takes in slice of key-value pair to construct a job id. e.g.
// key1--val1__key2--val2__key3--val3__.....
func (sv *gbqService) generateJobId(kv ...string) string {
	var buffer bytes.Buffer
	for i := 0; i < len(kv); i = i + 2 {
		buffer.WriteString(kv[i])
		buffer.WriteString(KeyValueSeparator)
		if i+1 < len(kv) {
			buffer.WriteString(kv[i+1])
		}
		buffer.WriteString(PairSeparator)
	}
	return buffer.String()
}


