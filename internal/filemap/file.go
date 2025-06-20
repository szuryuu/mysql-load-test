package main

import (
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"syscall"
)

type newLineMapping struct {
	file             *os.File
	positions        []int64
	cacheSizeInBytes int
}

func newNewLineMapping(file *os.File, cacheSizeInBytes int) *newLineMapping {
	return &newLineMapping{
		file:             file,
		cacheSizeInBytes: cacheSizeInBytes,
		positions:        make([]int64, 0),
	}
}

func pickRandom(buf []byte, file *os.File, positions []int64) (int, error) {
	// fmt.Println("len(positions): ", len(positions), "file", file.Name())
	r := rand.IntN(len(positions))
	var startOffset int64
	if r > 0 {
		startOffset = positions[r-1] + 1
	} else {
		startOffset = 0
	}
	endOffset := positions[r]
	length := endOffset - startOffset
	readN, readErr := readSegment(buf, file, int(startOffset), int(length))
	if readErr != nil {
		return 0, fmt.Errorf("failed to read segment from offset %d to %d", startOffset, endOffset)
	}
	return readN, nil
}

func readSegment(buf []byte, file *os.File, offset, length int) (int, error) {
	_, seekErr := file.Seek(int64(offset), 0)
	if seekErr != nil {
		return 0, fmt.Errorf("failed to seek file to offset %d: %v", offset, seekErr)
	}
	readN, readErr := file.Read(buf[:length])
	if readErr != nil {
		if readErr != io.EOF {
			return 0, readErr
		}
	}
	return readN, nil
}

func (m *newLineMapping) findNewLinesPositions() error {
	file := m.file

	_, err := file.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning: %v", err)
	}

	buffer := make([]byte, 50*1024*1024) // 50 MiB

	lastPos := int64(0)

	for {
		n, readErr := file.Read(buffer)
		if n > 0 {
			for i := 0; i < n; i++ {
				if buffer[i] == 10 { // newline
					currentPos := lastPos + int64(i)
					if i == 0 && lastPos != 0 {
						currentPos++
					}
					m.positions = append(m.positions, currentPos)
				}
			}
			lastPos += int64(n)
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("failed reading file: %v", readErr)
		}
	}

	return nil
}

func openFileWithReadLock(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed opening file: %v", err)
	}

	lock := syscall.Flock_t{
		Type:   syscall.F_RDLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
	}

	lockErr := syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock)
	if lockErr != nil {
		file.Close()
		return nil, fmt.Errorf("failed to acquire read lock: %v", lockErr)
	}

	return file, nil
}
