package bigquery

import (
	"context"
)

func IsContextCancelled(ctx context.Context) bool {
	if(ctx != nil && (ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded)) {
		return true
	}
	return false
}

func IsContextsCancelled(ctxs []context.Context) bool {
	for _,ctx := range ctxs {
		if IsContextCancelled(ctx) {
			return  true
		}
	}
	return false
}

