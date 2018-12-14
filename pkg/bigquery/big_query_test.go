package bigquery

import (
	"log"
	"os"
	"testing"
	"strings"
	"context"
	"time"

	"cloud.google.com/go/bigquery"
	"errors"
	"github.com/viant/assertly"
	_ "github.com/viant/bgc"
	"github.com/viant/dsunit"
	"fmt"
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
		FailRetry:  3,
	}
	status, msg, err := svc.Load(job, 10*time.Minute)
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

func TestGbqService_LoadCancelContextUseCases(t *testing.T) {
	type response struct {
		status *bigquery.JobStatus
		err    error
	}
	type usecase struct {
		description string
		skip        bool
		job    		*LoadJob
		wait   		time.Duration // wait to simulate external Cancel
		timeout		time.Duration // load Timeout to Big-Query
		expected    *response
		chk    		int
	}

	credential := "${env.HOME}/.secret/bq.json"
	credential = strings.Replace(credential, "${env.HOME}", os.Getenv("HOME"), 1)
	schema, err := SchemaFromFile("file://" + gopath + "/src/github.com/viant/etly/test/data/schema/SampleSchema.json")
	if err != nil {
		t.Fatalf("cannot load schema: %v", err)
	}
	URIs := []string{
		"gs://etly_test_errors_2/test/cancel_test_01.json.gz",
	}

	URIs_2 := []string{
		"gs://etly_test_errors_2/test/cancel_test_02.json.gz",
	}

	URIs_3 := []string{
		"gs://etly_test_errors_2/test/cancel_test_01.json.gz",
	}

	var useCases = []*usecase(nil)

	useCases = append(useCases,&usecase{
		description: "Load Job Cancel ",
		skip: false,
		expected: &response{
			status: nil,
			err: errors.New("context canceled"),

		},
		job: &LoadJob{
			ProjectID:  "tech-ops-poc",
			Schema:     schema,
			Credential: credential,
			DatasetID:  "etly_test",
			TableID:    "etly_errors_2",
			URIs:       URIs,
			FailRetry:  1,
		},
		wait:15 * time.Second, // NOT sufficient to load big 67MB file
		timeout:60 * time.Minute, //long timeout to test external cancel
		chk:dsunit.FullTableDatasetCheckPolicy, // We want to check full table to check nothing is inserted
	})

	useCases = append(useCases,&usecase{
		description: "Load Job Cancel Long Wait ",
		skip:false,
		expected: &response{
			status: &bigquery.JobStatus{
				State:bigquery.Done,
			},
			err: nil,

		},
		job: &LoadJob{
			ProjectID:  "tech-ops-poc",
			Schema:     schema,
			Credential: credential,
			DatasetID:  "etly_test",
			TableID:    "etly_errors_2",
			URIs:       URIs_2,
			FailRetry:  1,
		},
		wait:15 * time.Second, //sufficient to load small 140 bytes file
		timeout:60 * time.Minute, //long timeout to test external cancel
		chk:dsunit.SnapshotDatasetCheckPolicy,
	})


	useCases = append(useCases,&usecase{
		description: "Load Job Bigquery timeout ",
		skip: false,
		expected: &response{
			status: &bigquery.JobStatus{
				State:bigquery.Done,
			},
			err: nil,

		},
		job: &LoadJob{
			ProjectID:  "tech-ops-poc",
			Schema:     schema,
			Credential: credential,
			DatasetID:  "etly_test",
			TableID:    "etly_errors_3",
			URIs:       URIs_3,
			FailRetry:  1,
		},
		wait:10 * time.Minute, // Should get cancelled before the external cancellation occurs
		timeout:30 * time.Second, //short timeout to test bigquery timeout
		chk:dsunit.FullTableDatasetCheckPolicy, // We want to check full table to check nothing is inserted
	})

	for i,useCase := range useCases {
		j := i+1
		if useCase.skip {
			log.Printf("Skipping %v\n" ,  j)
			continue
		}
		if dsunit.InitFromURL(t, "test/config/init.yaml") {
			if !dsunit.PrepareFor(t, "etly_test", "test/data", fmt.Sprintf("use_case_%d",j)) {
				return
			}
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			svc := NewWithContext(ctx)
			go waiter(useCase.wait, cancel)
			resultStatus, jobId, resultErr := svc.Load(useCase.job, useCase.timeout)
			t.Log(" Exited from Job ", jobId)
			res := &response{
				status: resultStatus,
				err: resultErr,
			}
			assertly.AssertValues(t, useCase.expected, res)
			time.Sleep(time.Duration(5) * time.Minute)
			dsunit.ExpectFor(t, "etly_test", useCase.chk, "test/data", fmt.Sprintf("use_case_%d",j))
		}

	}


}


func waiter(waitTime time.Duration, cancel context.CancelFunc) {
	log.Println("Waiting to cancel Context... ")
	time.Sleep(waitTime)
	//cancel after loader Run finishes or test ends
	cancel()
	log.Println(" Called Context Cancel  ")
}








