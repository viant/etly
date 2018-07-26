package bigquery

import (
	"log"
	"os"
	"testing"
	"strings"
	"context"
	"time"
	"github.com/stretchr/testify/assert"

	"cloud.google.com/go/bigquery"
)

var gopath string

func init() {
	if gopath = os.Getenv("GOPATH"); gopath == "" {
		log.Panic("must define GOPATH")
	}
}

func TestGbqService_Load(t *testing.T) {
	//TODO: Create etly_test_errors bucket manually and upload test file.
	//TODO: Add service account (secret) as bucket owner/admin

	// Change credential to match local secret
	credential := gopath + "/src/github.com/viant/etly/test/secret/bq-upload_secret.json"
	svc := New()
	schema, err := SchemaFromFile("file://.../test/data/schema/SampleSchema.json")
	if err != nil {
		t.Fatalf("cannot load schema: %v", err)
	}
	URIs := []string{
		"gs://etly_test_errors/test/validjson01.gz",
		"gs://etly_test_errors/test/invalidjson01.gz",
		"gs://etly_test_errors/test/validjson02.gz",
		"gs://etly_test_errors/test/invalidgzip01.gz",
	}

	job := &LoadJob{
		ProjectID:  "viant-adelphic",
		Schema:     schema,
		Credential: credential,
		DatasetID:  "etly_test",
		TableID:    "etly_errors",
		URIs:       URIs,
	}
	status, msg, err := svc.Load(job)
	t.Logf("Status: %+v\n", status)
	t.Logf("Msg: %v\n", msg)
	if status.Err() != nil {
		t.Logf("Possible Error: %v", status.Err().Error())
	}
	for _, failedJob := range status.Errors {
		t.Logf("Error: %v\n", failedJob)
	}
	if err != nil {
		t.Fatalf("error loading to Bigquery %v", err)
	}
	//TODO: Clean up test files and tables
}

func TestGbqService_LoadCancelContext(t *testing.T) {
	//TODO: Create etly_test_errors bucket manually and upload test file.
	//TODO: Add service account (secret) as bucket owner/admin

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	// Change credential to match local secret
	//credential := gopath + "/src/github.com/viant/etly/test/secret/bq-upload_secret.json"
	credential := "${env.HOME}/.secret/bq.json"
	credential = strings.Replace(credential, "${env.HOME}", os.Getenv("HOME"), 1)
	svc := NewWithContext(ctx)
	schema, err := SchemaFromFile("file://" + gopath + "/src/github.com/viant/etly/test/data/schema/SampleSchema.json")
	if err != nil {
		t.Fatalf("cannot load schema: %v", err)
	}
	URIs := []string{
		"gs://etly_test_errors_2/test/cancel_test_01.json.gz",
	}

	job := &LoadJob{
		ProjectID:  "tech-ops-poc",
		Schema:     schema,
		Credential: credential,
		DatasetID:  "etly_test",
		TableID:    "etly_errors_2",
		URIs:       URIs,
	}
	go func() {
		log.Println("Waiting to cancel Context... ")
		time.Sleep(30 * time.Second)
		//cancel a little after loader Runs
		cancel()
		log.Println(" Called Context Cancel  ")
	}()
	status, jobId, err := svc.Load(job)
	t.Log(" Exited from Job ", jobId)
	assert.Nil(t, status)
	assert.NotNil(t, err)
	assert.Equal(t,"context canceled", err.Error())
	//TODO: Clean up test files and tables
}

func TestGbqService_LoadCancelContextLongWait(t *testing.T) { //Negative test for cancel, Expect table with data to be created
	//TODO: Create etly_test_errors bucket manually and upload test file.
	//TODO: Add service account (secret) as bucket owner/admin

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	// Change credential to match local secret
	//credential := gopath + "/src/github.com/viant/etly/test/secret/bq-upload_secret.json"
	credential := "${env.HOME}/.secret/bq.json"
	credential = strings.Replace(credential, "${env.HOME}", os.Getenv("HOME"), 1)
	svc := NewWithContext(ctx)
	schema, err := SchemaFromFile("file://" + gopath + "/src/github.com/viant/etly/test/data/schema/SampleSchema.json")
	if err != nil {
		t.Fatalf("cannot load schema: %v", err)
	}
	URIs := []string{
		"gs://etly_test_errors_2/test/cancel_test_01.json.gz",
	}

	job := &LoadJob{
		ProjectID:  "tech-ops-poc",
		Schema:     schema,
		Credential: credential,
		DatasetID:  "etly_test",
		TableID:    "etly_errors_2",
		URIs:       URIs,
	}
	go func() {
		log.Println("Waiting until test ends to cancel Context... ")
		time.Sleep(600 * time.Second)
		//cancel after loader Run finishes or test ends
		cancel()
		log.Println(" Called Context Cancel  ")
	}()
	status, jobId, err := svc.Load(job)
	t.Log(" Exited from Job ", jobId)
	assert.NotNil(t, status)
	assert.Nil(t, err)
	assert.Equal(t,status.State, bigquery.Done)
	//TODO: Clean up test files and tables
}





