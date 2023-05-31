/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package bench

import (
	"context"
	"fmt"
	"github.com/SpectraLogic/ds3_go_sdk/ds3"
	"github.com/SpectraLogic/ds3_go_sdk/ds3/models"
	"math"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
	"github.com/minio/warp/pkg/generator"
)

type Benchmark interface {
	// Prepare for the benchmark run
	Prepare(ctx context.Context) error

	// Start will execute the main benchmark.
	// Operations should begin executing when the start channel is closed.
	Start(ctx context.Context, wait chan struct{}) (Operations, error)

	// Clean up after the benchmark run.
	Cleanup(ctx context.Context)

	// Common returns the common parameters.
	GetCommon() *Common
}

// Common contains common benchmark parameters.
type Common struct {
	Client func() (cl *ds3.Client, done func())

	Concurrency int
	Source      func() generator.Source
	Bucket      string
	Endpoint    string
	Locking     bool

	// Running in client mode.
	ClientMode bool
	// Clear bucket before benchmark
	Clear           bool
	PrepareProgress chan float64
	// Does destination support versioning?
	Versioned bool

	// Auto termination is set when this is > 0.
	AutoTermDur   time.Duration
	AutoTermScale float64

	// Default Put options.
	PutOpts minio.PutObjectOptions

	// Custom is returned to server if set by clients.
	Custom map[string]string

	// Error should log an error similar to fmt.Print(data...)
	Error func(data ...interface{})

	// ClientIdx is the client index.
	// Will be 0 if single client.
	ClientIdx int

	// ExtraFlags contains extra flags to add to remote clients.
	ExtraFlags map[string]string
}

const (
	// Split active ops into this many segments.
	autoTermSamples = 25

	// Number of segments that must be within limit.
	// The last segment will be the one considered 'current speed'.
	autoTermCheck = 7
)

// GetCommon implements interface compatible implementation
func (c *Common) GetCommon() *Common {
	return c
}

// ErrorF formatted error printer
func (c *Common) ErrorF(format string, data ...interface{}) {
	c.Error(fmt.Sprintf(format, data...))
}

// createEmptyBucket will create an empty bucket
// or delete all content if it already exists.
func (c *Common) createEmptyBucket(ctx context.Context) error {
	cl, done := c.Client()
	defer done()

	headBucketRequest := models.NewHeadBucketRequest(c.Bucket)
	_, err := cl.HeadBucket(headBucketRequest)
	if err == nil {
		console.Eraseline()
		console.Infof("\rDeleting Bucket %q...", c.Bucket)

		deleteBucketRequest := models.NewDeleteBucketSpectraS3Request(c.Bucket)
		deleteBucketRequest.Force = true

		// don't capture the error; the bucket may not exist and the DS3 response
		// won't tell us the difference between "bucket wasn't there" and "bucket
		// was there but I couldn't delete it."
		cl.DeleteBucketSpectraS3(deleteBucketRequest)
	}

	console.Eraseline()
	console.Infof("\rCreating Bucket %q...", c.Bucket)

	putBucketRequest := models.NewPutBucketRequest(c.Bucket)
	_, err = cl.PutBucket(putBucketRequest)

	// In client mode someone else may have created it first.
	// Check if it exists now.
	// We don't test against a specific error since we might run against many different servers.
	if err != nil {
		headBucketRequest = models.NewHeadBucketRequest(c.Bucket)
		_, err2 := cl.HeadBucket(headBucketRequest)
		if err2 != nil {
			return err // return original error
		}
	}

	return nil
}

// prepareProgress updates preparation progess with the value 0->1.
func (c *Common) prepareProgress(progress float64) {
	if c.PrepareProgress == nil {
		return
	}
	progress = math.Max(0, math.Min(1, progress))
	select {
	case c.PrepareProgress <- progress:
	default:
	}
}
