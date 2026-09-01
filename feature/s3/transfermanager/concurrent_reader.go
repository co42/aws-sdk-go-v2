package transfermanager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// concurrentReader receives object parts from working goroutines, composes those chunks in order and read
// to user's buffer. ConcurrentReader limits the max number of chunks it could receive and read at the same
// time so getter won't send following parts' request to s3 until user reads all current chunks, which avoids
// too much memory consumption when caching large object parts
type concurrentReader struct {
	ch      chan outChunk
	buf     map[int32]*outChunk
	options Options
	in      *GetObjectInput

	pos          int64
	partsCount   int32
	capacity     int32
	sectionParts int32
	sendCount    int32
	receiveCount int32
	readCount    int32
	totalBytes   int64
	index        int32
	written      int64
	partSize     int64
	invocations  int32
	etag         *string

	startOnce  sync.Once
	capacityCh chan struct{}

	ctx context.Context
	m   sync.Mutex
	wg  sync.WaitGroup

	err error
}

// Read implements io.Reader to compose object parts in order and read to p.
// It will receive up to r.capacity chunks, read them to p if any chunk index
// fits into p scope, otherwise it will buffer those chunks and read them in
// following calls
func (r *concurrentReader) Read(p []byte) (int, error) {
	if err := r.getErr(); err != nil && err != io.EOF {
		return 0, err
	}

	r.startOnce.Do(r.start)

	written, err := r.read(p)
	if err != nil {
		r.setErr(err)
	}

	r.written += int64(written)
	r.invocations++
	return written, r.getErr()
}

// start launches the download pipeline once: persistent part-download
// workers plus a dispatcher that slides the request window forward as the
// consumer frees buffer capacity. Downloads therefore continue between and
// during Read calls, overlapping network transfer with the consumer's
// processing, instead of being dispatched in waves gated on each Read.
func (r *concurrentReader) start() {
	if r.capacityCh == nil {
		r.capacityCh = make(chan struct{}, 1)
	}

	clientOptions := []func(*s3.Options){
		func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions,
				middleware.AddSDKAgentKey(middleware.FeatureMetadata, userAgentKey),
				addFeatureUserAgent,
			)
		}}

	ch := make(chan getChunk, r.options.Concurrency)
	for i := 0; i < r.options.Concurrency; i++ {
		r.wg.Add(1)
		go r.downloadPart(r.ctx, ch, clientOptions...)
	}

	// dispatcher: request the next part as soon as the buffer window allows
	go func() {
		defer close(ch)
		for r.index < r.partsCount {
			if e := r.getErr(); e != nil && e != io.EOF {
				return
			}

			if r.index == atomic.LoadInt32(&r.capacity) {
				// wait for the consumer to free capacity; setErr also
				// signals capacityCh, and the getErr check above then
				// exits the loop
				select {
				case <-r.capacityCh:
				case <-r.ctx.Done():
					return
				}
				continue
			}

			var chunk getChunk
			if r.options.GetObjectType == types.GetObjectParts {
				chunk = getChunk{part: r.index + 1, index: r.index}
			} else {
				chunk = getChunk{withRange: r.byteRange(), index: r.index}
			}
			select {
			case ch <- chunk:
			case <-r.ctx.Done():
				return
			}

			r.pos += r.partSize
			r.index++
		}
	}()

	// close the receive channel once every worker has exited (completion,
	// error, or cancellation), so the consumer can never block on a channel
	// nothing will send to; buffered chunks stay readable after close.
	go func() {
		r.wg.Wait()
		close(r.ch)
	}()
}

// extendCapacity slides the buffer window forward — one part of new capacity
// per fully consumed part, keeping at most sectionParts parts buffered — and
// wakes the dispatcher.
func (r *concurrentReader) extendCapacity() {
	capacity := min(r.readCount+r.sectionParts, r.partsCount)
	atomic.StoreInt32(&r.capacity, capacity)
	select {
	case r.capacityCh <- struct{}{}:
	default:
	}
}

func (r *concurrentReader) downloadPart(ctx context.Context, ch chan getChunk, clientOptions ...func(*s3.Options)) {
	defer r.wg.Done()
	for {
		chunk, ok := <-ch
		if !ok {
			break
		}
		if r.getErr() != nil {
			continue
		}
		_, err := r.downloadChunk(ctx, chunk, clientOptions...)
		if err != nil {
			r.setErr(err)
		}
	}
}

// downloadChunk downloads the chunk from s3
func (r *concurrentReader) downloadChunk(ctx context.Context, chunk getChunk, clientOptions ...func(*s3.Options)) (*GetObjectOutput, error) {
	params := r.in.mapGetObjectInput(!r.options.DisableChecksumValidation)
	if chunk.part != 0 {
		params.PartNumber = aws.Int32(chunk.part)
	}
	if chunk.withRange != "" {
		params.Range = aws.String(chunk.withRange)
	}
	if params.VersionId == nil {
		params.IfMatch = r.etag
	}

	out, err := r.options.S3.GetObject(ctx, params, clientOptions...)
	if err != nil {
		return nil, err
	}

	if params.Range != nil && out.ContentRange != nil {
		reqStart, reqEnd, err := getReqRange(aws.ToString(params.Range))
		if err != nil {
			return nil, err
		}
		respStart, respEnd, err := getRespRange(aws.ToString(out.ContentRange))
		if err != nil {
			return nil, err
		}
		// don't validate first chunk since object size is unknown when getting that
		if reqStart != 0 && (reqStart != respStart || reqEnd != respEnd) {
			return nil, fmt.Errorf("range mismatch between request %d-%d and response %d-%d", reqStart, reqEnd, respStart, respEnd)
		}
	}

	defer out.Body.Close()
	buf, err := io.ReadAll(out.Body)

	if err != nil {
		return nil, err
	}
	// cancelable: an abandoned reader must not leave workers blocked on a
	// channel nobody drains
	select {
	case r.ch <- outChunk{body: bytes.NewReader(buf), index: chunk.index, length: aws.ToInt64(out.ContentLength)}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	output := &GetObjectOutput{}
	output.mapFromGetObjectOutput(out, params.ChecksumMode)
	return output, err
}

// byteRange returns an HTTP Byte-Range header value that should be used by the
// client to request a chunk range.
func (r *concurrentReader) byteRange() string {
	return fmt.Sprintf("bytes=%d-%d", r.pos, min(r.totalBytes-1, r.pos+r.partSize-1))
}

type getChunk struct {
	part      int32
	withRange string

	index int32
}

type outChunk struct {
	body  io.Reader
	index int32

	length int64
	cur    int64
}

// read copies sequential object data into p, returning as soon as any data
// is available: it blocks only until the chunk containing the current offset
// has been received, never for the rest of the window — downloads of the
// following parts continue in the background while the caller processes p.
func (r *concurrentReader) read(p []byte) (int, error) {
	if cap(p) == 0 {
		return 0, nil
	}

	var written int

	for written < cap(p) {
		if e := r.getErr(); e != nil && e != io.EOF {
			r.clean()
			return written, r.getErr()
		}

		cur := int32((r.written + int64(written)) / r.partSize)
		c, ok := r.buf[cur]
		if !ok {
			if r.getErr() == io.EOF {
				break
			}
			if written > 0 {
				// p has data and the next chunk isn't buffered yet:
				// return instead of blocking for more
				select {
				case oc, chOk := <-r.ch:
					if !chOk {
						return written, r.getErr()
					}
					r.receiveCount++
					r.buf[oc.index] = &oc
					continue
				default:
					return written, nil
				}
			}
			oc, chOk := <-r.ch
			if !chOk {
				// closed: every worker exited (completion, error, or cancel)
				if e := r.getErr(); e != nil {
					return written, e
				}
				return written, io.EOF
			}
			r.receiveCount++
			r.buf[oc.index] = &oc
			continue
		}

		n, err := c.body.Read(p[written:])
		c.cur += int64(n)
		written += n
		if err != nil && err != io.EOF {
			r.setErr(err)
			r.clean()
			return written, r.getErr()
		}
		if c.cur >= c.length {
			r.readCount++
			delete(r.buf, cur)
			r.extendCapacity()
			if r.readCount >= r.partsCount {
				r.setErr(io.EOF)
			}
		}
	}

	return written, r.getErr()
}

func (r *concurrentReader) setErr(err error) {
	r.m.Lock()
	defer r.m.Unlock()

	r.err = err
	if err != nil && err != io.EOF && r.capacityCh != nil {
		// wake the dispatcher if it is waiting for capacity; capacityCh is
		// 1-buffered so the signal is retained even if it isn't blocked yet
		select {
		case r.capacityCh <- struct{}{}:
		default:
		}
	}
}

func (r *concurrentReader) getErr() error {
	r.m.Lock()
	defer r.m.Unlock()

	return r.err
}

func (r *concurrentReader) clean() {
	r.buf = nil
	for {
		_, ok := <-r.ch
		if !ok {
			break
		}
	}
}
