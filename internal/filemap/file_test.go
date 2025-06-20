package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestNewNewLineMapping(t *testing.T) {
	tempFile, err := createTempFileWithContent("line1\nline2\nline3\n")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	nlm := newNewLineMapping(tempFile, 1024)
	if nlm == nil {
		t.Fatal("Expected newNewLineMapping to return a non-nil value")
	}
	if nlm.file != tempFile {
		t.Error("Expected file to be set correctly")
	}
	if nlm.cacheSizeInBytes != 1024 {
		t.Errorf("Expected cacheSizeInBytes to be 1024, got %d", nlm.cacheSizeInBytes)
	}
	if len(nlm.positions) != 0 {
		t.Errorf("Expected positions to be initialized as empty, got length %d", len(nlm.positions))
	}
}

func TestFindNewLinesPositions(t *testing.T) {
	testCases := []struct {
		name     string
		content  string
		expected []int64
	}{
		{
			name:     "Empty file",
			content:  "",
			expected: []int64{},
		},
		{
			name:     "Single line",
			content:  "hello world",
			expected: []int64{},
		},
		{
			name:     "Single line with newline",
			content:  "hello world\n",
			expected: []int64{11},
		},
		{
			name:     "Multiple lines",
			content:  "line1\nline2\nline3\n",
			expected: []int64{5, 11, 17},
		},
		{
			name:     "Multiple lines without final newline",
			content:  "line1\nline2\nline3",
			expected: []int64{5, 11},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempFile, err := createTempFileWithContent(tc.content)
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tempFile.Name())
			defer tempFile.Close()

			nlm := newNewLineMapping(tempFile, 1024)
			err = nlm.findNewLinesPositions()
			if err != nil {
				t.Fatalf("findNewLinesPositions failed: %v", err)
			}

			if len(nlm.positions) != len(tc.expected) {
				t.Fatalf("Expected %d positions, got %d: %v", len(tc.expected), len(nlm.positions), nlm.positions)
			}

			for i, pos := range tc.expected {
				if nlm.positions[i] != pos {
					t.Errorf("Position %d: expected %d, got %d", i, pos, nlm.positions[i])
				}
			}
		})
	}
}

func TestReadSegment(t *testing.T) {
	content := "line1\nline2\nline3\n"
	tempFile, err := createTempFileWithContent(content)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	nlm := newNewLineMapping(tempFile, 1024)

	testCases := []struct {
		name           string
		offset         int
		length         int
		expectedData   string
		expectedLength int
		expectError    bool
	}{
		{
			name:           "Read first line",
			offset:         0,
			length:         6, // "line1\n"
			expectedData:   "line1\n",
			expectedLength: 6,
			expectError:    false,
		},
		{
			name:           "Read second line",
			offset:         6,
			length:         6, // "line2\n"
			expectedData:   "line2\n",
			expectedLength: 6,
			expectError:    false,
		},
		{
			name:           "Read partial",
			offset:         3,
			length:         4, // "e1\nl"
			expectedData:   "e1\nl",
			expectedLength: 4,
			expectError:    false,
		},
		{
			name:           "Read beyond file",
			offset:         len(content) + 10,
			length:         5,
			expectedData:   "",
			expectedLength: 0,
			expectError:    false, // EOF is not considered an error in readSegment
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 100)
			n, err := nlm.readSegment(buf, tc.offset, tc.length)

			if tc.expectError && err == nil {
				t.Fatal("Expected an error but got none")
			}
			if !tc.expectError && err != nil && err != io.EOF {
				t.Fatalf("Unexpected error: %v", err)
			}
			if n != tc.expectedLength {
				t.Errorf("Expected to read %d bytes, got %d", tc.expectedLength, n)
			}
			if string(buf[:n]) != tc.expectedData {
				t.Errorf("Expected data '%s', got '%s'", tc.expectedData, string(buf[:n]))
			}
		})
	}
}

func TestPickRandom(t *testing.T) {
	content := "line1\nline2\nline3\nline4\n"
	tempFile, err := createTempFileWithContent(content)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	nlm := newNewLineMapping(tempFile, 1024)
	err = nlm.findNewLinesPositions()
	if err != nil {
		t.Fatalf("Failed to find newline positions: %v", err)
	}

	// Verify positions are correct
	expectedPositions := []int64{5, 11, 17, 23}
	if len(nlm.positions) != len(expectedPositions) {
		t.Fatalf("Expected %d positions, got %d", len(expectedPositions), len(nlm.positions))
	}
	for i, pos := range expectedPositions {
		if nlm.positions[i] != pos {
			t.Errorf("Position %d: expected %d, got %d", i, pos, nlm.positions[i])
		}
	}

	// Test pickRandom
	buf := make([]byte, 100)
	for i := 0; i < 10; i++ { // Run multiple times to increase chance of covering different random selections
		err = nlm.pickRandom(buf)
		if err != nil {
			t.Fatalf("pickRandom failed: %v", err)
		}

		// Verify that the picked line is one of the expected lines
		picked := string(bytes.TrimRight(buf, "\x00"))
		validLines := []string{"line1", "line2", "line3", "line4"}

		found := false
		for _, line := range validLines {
			if picked == line {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Picked unexpected line: '%s'", picked)
		}
	}
}

func TestOpenFileWithReadLock(t *testing.T) {
	tempFile, err := createTempFileWithContent("test content")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close() // Close it so we can reopen with our function

	file, err := openFileWithReadLock(tempFile.Name())
	if err != nil {
		t.Fatalf("openFileWithReadLock failed: %v", err)
	}
	defer file.Close()

	// Test that we can read from the file
	buf := make([]byte, 100)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read from locked file: %v", err)
	}
	if string(buf[:n]) != "test content" {
		t.Errorf("Expected 'test content', got '%s'", string(buf[:n]))
	}

	// Test with non-existent file
	_, err = openFileWithReadLock("/non/existent/file")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

// Helper function to create a temporary file with the given content
func createTempFileWithContent(content string) (*os.File, error) {
	tempFile, err := os.CreateTemp("", "nlm_test_*.txt")
	if err != nil {
		return nil, err
	}

	_, err = tempFile.WriteString(content)
	if err != nil {
		os.Remove(tempFile.Name())
		tempFile.Close()
		return nil, err
	}

	_, err = tempFile.Seek(0, 0)
	if err != nil {
		os.Remove(tempFile.Name())
		tempFile.Close()
		return nil, err
	}

	return tempFile, nil
}
