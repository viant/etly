package bigquery

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"
	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
	"os"
	"log"
	"math"
	"fmt"
)

// Service provides loading capability from Cloud Storage to BigQuery
type Service interface {
	// Load performs a gbq loading job. This method is a blocking operation so it is ideal
	// to be executed in a go routine.
	Load(loadJob *LoadJob, timeout time.Duration) (*bigquery.JobStatus, string, error)
}

type gbqService struct{
	context context.Context
}

// LoadJob contains all necessary information for GBQ Service to perform its task
type LoadJob struct {
	Credential string
	TableID    string
	DatasetID  string
	ProjectID  string
	Schema     bigquery.Schema
	URIs       []string
	FailRetry  int
}

const (
	// KVSeparator is a key-value separator e.g key1--val1__key2--val2__key3--val3__.....
	KVSeparator = "--"
	// PairSeparator is a separator for each K-V pair e.g key1--val1__key2--val2__key3--val3__.....
	PairSeparator = "__"
	// ErrorDuplicate contains text indicating GBQ Duplication Error
	ErrorDuplicate = "Error 409"
)

// New constructs a bigquery service
func New() Service {
	return &gbqService{
		context.TODO(),
	}
}

// New constructs a bigquery service
func NewWithContext(context context.Context) Service {
	return &gbqService{
		context,
	}
}

func (sv *gbqService) Load(loadJob *LoadJob, timeout time.Duration) (*bigquery.JobStatus, string, error) {
	jobID := sv.generateJobID(
		"ProjectID", loadJob.ProjectID,
		"DatasetID", loadJob.DatasetID,
		"TableID", loadJob.TableID,
		"Ts", strconv.FormatInt(time.Now().Unix(), 10))
	var status *bigquery.JobStatus
	var err error
	status, err = sv.loadJobId(loadJob, jobID, timeout)

	if err != nil {
		log.Printf(" Error Big Query loadJob attempt for jobId %v due to %v  ",jobID, err.Error())
	} else if (IsContextCancelled(sv.context)) {
		log.Printf(" Cancelled Big Query loadJob attempt for jobId %v ",jobID)
	}
	return status,jobID,err
}

func (sv *gbqService) loadJobId(loadJob *LoadJob, jobID string, timeout time.Duration) (status *bigquery.JobStatus, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ERROR recovered from panic bigquery-JobId: %v Message: %v", jobID, r.(error))
			status = nil
		}
	}()
	credential := strings.Replace(loadJob.Credential, "${env.HOME}", os.Getenv("HOME"), 1)
	clientOption := option.WithServiceAccountFile(credential)
	//ctx := context.Background() // Now passed from upstream via Constructor to perform graceful shutdown

	client, err := bigquery.NewClient(sv.context, loadJob.ProjectID, clientOption)
	if err != nil {
		return nil,  err
	}
	defer client.Close()
	ref := bigquery.NewGCSReference(loadJob.URIs...)
	if loadJob.Schema == nil {
		ref.AutoDetect = true
	} else {
		ref.Schema = loadJob.Schema
	}
	ref.SourceFormat = bigquery.JSON
	dataset := client.DatasetInProject(loadJob.ProjectID, loadJob.DatasetID)
	if err := dataset.Create(sv.context, nil); err != nil {
		// Create dataset if it does exist, otherwise ignore duplicate error
		if !strings.Contains(err.Error(), ErrorDuplicate) {
			return nil,  err
		}
	}
	loader := dataset.Table(loadJob.TableID).LoaderFrom(ref)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	loader.WriteDisposition = bigquery.WriteAppend
	loader.JobID = jobID
	job, err := loader.Run(sv.context)
	if err != nil {
		return nil,  err
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(sv.context, timeout)
	log.Printf("Waiting on bigquery jobId(%v) for duration(%v) \n", jobID, timeout)

	status, err = job.Wait(timeoutCtx)

	defer cancelJob(job, err, sv.context, timeoutCtx);
	//defers are executed LIFO order
	defer timeoutCancel()

	if err != nil && status == nil && timeoutCtx.Err() != context.DeadlineExceeded {
		//error while retreiving status
		log.Printf(" Error getting Job Status for jobId %v due to %v  Re-trying to get Status ",jobID, err.Error())
		for i:=0; i<loadJob.FailRetry; i++ {
			status,err = job.Wait(sv.context); // Wait calls Status()
			if err != nil && sv.context.Err() == nil {
				log.Printf(" Error attempt %v getting Job Status for jobId %v due to %v ",i+1,jobID, err.Error())
			} else {
				break;
			}
			time.Sleep(time.Duration(math.Pow(3,float64(i+1))) * time.Second)
		}
	}
	return status,  err
}

// Generate job ID following best practices:
// https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects#generate-jobID
// This method takes in slice of key-value pair to construct a job id. e.g.
// key1--val1__key2--val2__key3--val3__.....
func (sv *gbqService) generateJobID(kv ...string) string {
	var buffer bytes.Buffer
	for i := 0; i < len(kv); i += 2 {
		buffer.WriteString(kv[i])
		buffer.WriteString(KVSeparator)
		if i+1 < len(kv) {
			buffer.WriteString(kv[i+1])
		}
		buffer.WriteString(PairSeparator)
	}
	return buffer.String()
}

func cancelJob(job *bigquery.Job, err error, ctxs ...context.Context)  {
	isContextsCanclled := IsContextsCancelled(ctxs)
	if err != nil && isContextsCanclled {
		log.Printf(" Cancelling Job %v due to %v \n", job.ID(), err.Error())
		if cancelErr := job.Cancel(context.Background()); cancelErr != nil { // Cannot reuse context once cancelled, hence, create new
			err = cancelErr
		}
	}
}
