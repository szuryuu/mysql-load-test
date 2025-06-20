package main

import "os"

type FilePool struct {
	handles chan *os.File
	path    string
}

func NewFilePool(path string, maxHandles int) (*FilePool, error) {
	pool := &FilePool{
		handles: make(chan *os.File, maxHandles),
		path:    path,
	}

	return pool, nil
}

// Get returns a file handle from the pool
func (p *FilePool) Get() *os.File {
	return <-p.handles
}

// Put returns a file handle to the pool
func (p *FilePool) Put(file *os.File) {
	// Reset position to beginning
	file.Seek(0, 0)
	p.handles <- file
}

// Close closes all file handles
func (p *FilePool) Close() {
	close(p.handles)
	for file := range p.handles {
		file.Close()
	}
}
