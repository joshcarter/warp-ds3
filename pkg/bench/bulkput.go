package bench

import (
	"context"
	"github.com/SpectraLogic/ds3_go_sdk/ds3/models"
	"github.com/SpectraLogic/ds3_go_sdk/helpers"
	"github.com/joshcarter/warp-ds3/pkg/generator"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type BulkPut struct {
	Common
	BulkNum int
}

// Prepare will create an empty bucket ot delete any content already there.
func (u *BulkPut) Prepare(ctx context.Context) error {
	return u.createEmptyBucket(ctx)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (u *BulkPut) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(u.Concurrency)
	c := NewCollector()
	if u.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPut, u.AutoTermScale, autoTermCheck, autoTermSamples, u.AutoTermDur)
	}

	for i := 0; i < u.Concurrency; i++ {
		go func(i int) {
			rcv := c.Receiver()
			defer wg.Done()
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}

				objs := make(map[string]*generator.Object, u.BulkNum)
				ds3objs := make([]models.Ds3PutObject, u.BulkNum)
				totalSize := int64(0)

				for j := 0; j < u.BulkNum; j++ {
					obj := u.Source().Object() // Create a new generator for each object
					objs[obj.Name] = obj
					ds3objs[j] = models.Ds3PutObject{obj.Name, obj.Size}
					totalSize += obj.Size
				}

				op := Operation{
					OpType:   "BULKPUT",
					Thread:   uint16(i),
					Size:     totalSize,
					File:     ds3objs[0].Name,
					ObjPerOp: u.BulkNum,
					Endpoint: u.Endpoint,
				}
				op.Start = time.Now()
				client, cldone := u.Client()

				putBulkRequest := models.NewPutBulkJobSpectraS3Request(u.Bucket, ds3objs)

				putBulkResponse, err := client.PutBulkJobSpectraS3(putBulkRequest)
				if err != nil {
					u.Error("put bulk error: ", err)
					op.Err = err.Error()
				}

				totalChunkCount := len(putBulkResponse.MasterObjectList.Objects)
				curChunkCount := 0

				for curChunkCount < totalChunkCount {
					// fmt.Printf("chunk %d of %d\n", curChunkCount+1, totalChunkCount)

					// Get the list of available chunks that the server can receive. The server may
					// not be able to receive everything, so not all chunks will necessarily be
					// returned
					chunksReady := models.NewGetJobChunksReadyForClientProcessingSpectraS3Request(putBulkResponse.MasterObjectList.JobId)
					chunksReadyResponse, err := client.GetJobChunksReadyForClientProcessingSpectraS3(chunksReady)
					if err != nil {
						log.Fatal(err)
					}

					// Check to see if any chunks can be processed
					numberOfChunks := len(chunksReadyResponse.MasterObjectList.Objects)
					if numberOfChunks > 0 {
						// fmt.Printf("number of chunks: %d\n", numberOfChunks)

						// Loop through all the chunks that are available for processing, and send
						// the files that are contained within them.
						for _, curChunk := range chunksReadyResponse.MasterObjectList.Objects {
							for _, curObj := range curChunk.Objects {
								// fmt.Printf("- chunk %d of %d, object %d of %d\n", x+1, len(chunksReadyResponse.MasterObjectList.Objects), y+1, len(curChunk.Objects))
								r := objs[*curObj.Name].Reader
								r.Seek(curObj.Offset, io.SeekStart)
								reader := helpers.NewIoReaderWithSizeDecorator(r, curObj.Length)

								// fmt.Printf("- sending '%s' offset %d length %d\n", *curObj.Name, curObj.Offset, curObj.Length)
								putObjRequest := models.NewPutObjectRequest(u.Bucket, *curObj.Name, reader).
									WithJob(chunksReadyResponse.MasterObjectList.JobId).
									WithOffset(curObj.Offset)

								_, err = client.PutObject(putObjRequest)
								if err != nil {
									op.Err = err.Error()
									log.Fatal(err)
								}
								// fmt.Printf("Sent: %s offset=%d length=%d\n", *curObj.Name, curObj.Offset, curObj.Length)
							}
							curChunkCount++
						}
					} else {
						// When no chunks are returned we need to sleep to allow for cache space to
						// be freed.
						time.Sleep(time.Second * 5)
					}
				}
				op.End = time.Now()
				cldone()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (u *BulkPut) Cleanup(context.Context) {
	// u.deleteAllInBucket(ctx)
}
