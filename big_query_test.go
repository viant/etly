package etly

import (
	"log"
	"os"
	"testing"
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
	svc := NewBigqueryService()
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
