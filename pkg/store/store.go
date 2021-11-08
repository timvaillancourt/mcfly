package store

import (
	"bytes"
	"io"
	"os"
	"sync"
)

type indexPoint struct {
	Offset int64
	Bytes  int
}

type Store struct {
	sync.Mutex
	data        *os.File
	offset      int64
	indexPoints []indexPoint
	unreadBuf   *bytes.Buffer
}

func NewFileStore(dataFile string) (*Store, error) {
	store, err := os.Create(dataFile)
	if err != nil {
		return nil, err
	}

	return &Store{
		data:        store,
		indexPoints: make([]indexPoint, 0),
		unreadBuf:   &bytes.Buffer{},
	}, nil
}

func (s *Store) Close() {
	if s.data != nil {
		s.data.Close()
	}
}

func (s *Store) nextReverseIndexPoint() (point indexPoint, found bool) {
	indexPointsLen := len(s.indexPoints)
	if indexPointsLen > 0 {
		point = s.indexPoints[indexPointsLen-1]
		if indexPointsLen == 0 {
			s.indexPoints = []indexPoint{}
		} else {
			s.indexPoints = s.indexPoints[:indexPointsLen-1]
		}
		found = true
	}
	return point, found
}

func (s *Store) Write(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()

	n, err = s.data.Write(p)
	if err != nil {
		return n, err
	}

	s.indexPoints = append(s.indexPoints, indexPoint{
		Offset: s.offset,
		Bytes:  n,
	})
	s.offset += int64(n)

	return n, err
}

func (s *Store) Read(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()

	// empty unread buffer
	for s.unreadBuf.Len() > 0 {
		b, err := s.unreadBuf.ReadByte()
		if err != nil {
			return n, err
		}
		if n < len(p) {
			p[n] = b
			n += 1
		} else {
			return n, nil
		}
	}

	// get next reverse index point or return EOF if none exist
	indexPoint, found := s.nextReverseIndexPoint()
	if !found {
		return n, io.EOF
	}
	s.offset = indexPoint.Offset
	_, err = s.data.Seek(s.offset, 0)
	if err != nil {
		return n, err
	}

	// read bytes from index point
	bytes := make([]byte, indexPoint.Bytes)
	_, err = s.data.Read(bytes)
	if err != nil {
		return n, err
	}

	// copy 'bytes' -> 'p' or write to unread buffer if 'p' is full
	var i int
	for i < len(bytes) {
		if n < len(p) {
			p[n] = bytes[i]
			n += 1
		} else {
			s.unreadBuf.Write([]byte{bytes[i]})
		}
		i += 1
	}

	return n, nil
}
