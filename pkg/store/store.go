package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"os"
	"sync"
)

type FileStore struct {
	sync.Mutex
	file      *os.File
	offset    int64
	unreadBuf *bytes.Buffer
}

type FileStoreHeader struct {
	Offset int64
	Length int64
}

func NewFileStore(dataFile string) (*FileStore, error) {
	f, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	log.Printf("NewFileStore: opened file %q with length %d", dataFile, info.Size())

	return &FileStore{
		file:      f,
		offset:    info.Size(),
		unreadBuf: &bytes.Buffer{},
	}, err
}

func (fs *FileStore) Close() {
	if fs.file != nil {
		fs.file.Close()
	}
}

func (fs *FileStore) writeHeader(written int) error {
	return binary.Write(fs.file, binary.LittleEndian, int64(written))
}

func (fs *FileStore) readNextHeader() (header FileStoreHeader, err error) {
	if fs.offset <= binary.MaxVarintLen64 {
		return header, errors.New("invalid length")
	}

	//data := make([]byte, binary.MaxVarintLen64)
	var dataLength int64
	readOffset := fs.offset - binary.MaxVarintLen64
	log.Printf("readNextHeader: scanning from offset %d to %d", fs.offset, readOffset)
	//if _, err = fs.file.ReadAt(data, readOffset); err != nil {
	//	return header, err
	//}
	if _, err = fs.file.Seek(readOffset, 0); err != nil {
		return header, err
	}
	if err = binary.Read(fs.file, binary.LittleEndian, &dataLength); err != nil {
		return header, err
	}
	log.Printf("readNextHeader: got data length %d", dataLength)

	//length, n := binary.Varint(data)
	//if n != len(data) {
	//	return header, errors.New("invalid header")
	//}

	return FileStoreHeader{
		Length: dataLength,
		Offset: readOffset - dataLength,
	}, err
}

func (fs *FileStore) Write(p []byte) (n int, err error) {
	fs.Lock()
	defer fs.Unlock()

	n, err = fs.file.Write(p)
	if err != nil {
		return n, err
	}

	if err = fs.writeHeader(n); err != nil {
		return n, err
	}
	fs.offset += int64(n + binary.MaxVarintLen64)
	return n, err
}

func (fs *FileStore) Read(p []byte) (n int, err error) {
	fs.Lock()
	defer fs.Unlock()

	// empty unread buffer
	for fs.unreadBuf.Len() > 0 {
		b, err := fs.unreadBuf.ReadByte()
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

	// get next header
	header, err := fs.readNextHeader()
	if err != nil {
		return n, err
	}

	// read bytes
	bytes := make([]byte, header.Length)
	_, err = fs.file.ReadAt(bytes, header.Offset)
	if err != nil {
		return n, err
	}
	fs.offset = header.Offset

	// copy 'bytes' -> 'p' or write to unread buffer if 'p' is full
	var i int
	for i < len(bytes) {
		if n < len(p) {
			p[n] = bytes[i]
			n += 1
		} else {
			fs.unreadBuf.Write([]byte{bytes[i]})
		}
		i += 1
	}

	return n, nil
}
