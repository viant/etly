package etly

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_expandDateExpressionIfPresent(t *testing.T) {
	now := time.Unix(0, 1497277950*1000000000)
	expanded := expandDateExpressionIfPresent("!!<dateFormat:yyyyMMdd>!! ", &now)
	assert.Equal(t, "!!20170612!! ", expanded)
}

func Test_expandModExpressionIfPresent(t *testing.T) {
	expanded := expandModExpressionIfPresent("gs://b/20170612/<mod:40>/02-adlog.perf.log.2017-06-12_02-00.0.i-01972b29fe0657d40.gz", hash("adlog.perf.log.2017-06-12_02-00.0.i-01972b29fe0657d40.gz"))
	assert.Equal(t, "gs://b/20170612/18/02-adlog.perf.log.2017-06-12_02-00.0.i-01972b29fe0657d40.gz", expanded)
}

func Test_expandEnvironmentVariables(t *testing.T) {
	s := "s3://etl-endly/${env.USER}/adlog/<dateFormat:yyyy>/<dateFormat:MM>/<dateFormat:dd>/"
	v := expandEnvironmentVariableIfPresent(s)
	assert.Equal(t, -1, strings.Index(v, userVariableExpr))
}
