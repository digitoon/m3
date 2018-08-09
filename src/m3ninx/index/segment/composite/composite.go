// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package composite

import (
	"errors"
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
)

var (
	errSegmentClosed = errors.New("unable to perform operation, segment is closed")
)

// compositeSegment abstracts multiple segments into a single segment. It relies
// on the ordered iteration provided by FSTs to reduce the memory required to
// afford such an abstraction.
//
// The doc ID transformation looks as follows -
// input:
//                size   min    max
// subSegment-1   10     10     20
// subSegment-2   100    1      101
// sugSegment-3   50     1      51
//
// output:
//                size   min    max
// composite      160    1      161
// [1,11)    <- subSegment-1
// [11,111)  <- subSegment-2
// [111,161) <- subSegment-3

// NewSegment returns a new composite segment backed by the provided segments.
func NewSegment(segments ...sgmt.Segment) sgmt.Segment {
	return &compositeSegment{}
}

type compositeSegment struct {
	sync.RWMutex
	closed bool

	subs   []subSegment
	size   int64
	limits segmentLimits
}

var _ sgmt.Segment = &compositeSegment{}
var _ index.Reader = &compositeSegment{}

type subSegment struct {
	idx    int
	seg    sgmt.Segment
	reader index.Reader
	// internalLimits refer to the DocID range returned by the raw segment
	internalLimits segmentLimits
	// externalLimits refer to the transposed DocID exposed to the external
	// users of the compositeSegment
	externalLimits segmentLimits
}

type segmentLimits struct {
	startInclusive postings.ID
	endExclusive   postings.ID
}

func (c *compositeSegment) Size() int64 {
	c.RLock()
	defer c.RUnlock()
	return c.size
}

func (c *compositeSegment) ContainsID(docID []byte) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return false, errSegmentClosed
	}
	for _, s := range c.subs {
		contains, err := s.seg.ContainsID(docID)
		if err != nil {
			return false, err
		}
		if contains {
			return true, nil
		}
	}
	return false, nil
}

func (c *compositeSegment) Reader() (index.Reader, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return nil, errSegmentClosed
	}
	return c, nil
}

func (c *compositeSegment) Fields() (sgmt.FieldsIterator, error) {
	panic("not implemented")
}

func (c *compositeSegment) Terms(field []byte) (sgmt.TermsIterator, error) {
	panic("not implemented")
}

func (c *compositeSegment) Close() error {
	panic("not implemented")
}

func (c *compositeSegment) Doc(id postings.ID) (doc.Document, error) {
	panic("not implemented")
}

func (c *compositeSegment) MatchTerm(field []byte, term []byte) (postings.List, error) {
	panic("not implemented")
}

func (c *compositeSegment) MatchRegexp(
	field []byte,
	regexp []byte,
	compiled *index.CompiledRegex,
) (postings.List, error) {
	panic("not implemented")
}

func (c *compositeSegment) MatchAll() (postings.MutableList, error) {
	panic("not implemented")
}

func (c *compositeSegment) Docs(pl postings.List) (doc.Iterator, error) {
	panic("not implemented")
}

func (c *compositeSegment) AllDocs() (index.IDDocIterator, error) {
	panic("not implemented")
}
