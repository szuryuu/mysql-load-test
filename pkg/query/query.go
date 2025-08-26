package query

import (
	"encoding/binary"
	"fmt"
)

type Query struct {
	Raw                 []byte `json:"raw"`
	Fingerprint         []byte `json:"fingerprint"`
	Hash                uint64 `json:"hash"`
	Timestamp           uint64 `json:"timestamp"`
	FingerprintHash     uint64 `json:"fingerprint_hash"`
	CompletelyProcessed bool   `json:"completely_processed"`
	Offset              uint64 `json:"offset"`
	Length              uint64 `json:"length"`
}

const (
	_BEGIN_MARK = 0x1821781f

	_BEGIN_MARK_OFF           = 8
	_COMPLETELY_PROCESSED_OFF = 1
	_RAW_LENGTH_OFF           = _COMPLETELY_PROCESSED_OFF + 8
	_FINGERPRINT_LENGTH_OFF   = _RAW_LENGTH_OFF + 8
	_TIMESTAMP_OFF            = _FINGERPRINT_LENGTH_OFF + 8
	_HASH_OFF                 = _TIMESTAMP_OFF + 8
	_FINGERPRINT_HASH_OFF     = _HASH_OFF + 8
	_OFFSET_OFF               = _FINGERPRINT_HASH_OFF + 8
	_LENGTH_OFF               = _OFFSET_OFF + 8
	_HEADER_END_OFF           = _FINGERPRINT_HASH_OFF + 8
)

func (q *Query) GetSize() int {
	size := 0
	size += 8  // Beginning mark
	size += 1  // CompletelyProcessed
	size += 16 // Header
	size += 8  // Timestamp
	size += 8  // Hash
	size += 8  // FingerprintHash
	size += 8  // Offset
	size += 8  // Length
	size += len(q.Raw)
	size += len(q.Fingerprint)
	return size
}

func (q *Query) MarshalBinary(buf []byte) (int, error) {
	if len(buf) < q.GetSize() {
		return 0, fmt.Errorf("buffer too small: %d < %d", len(buf), q.GetSize())
	}

	if q.CompletelyProcessed {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	binary.PutUvarint(buf[_COMPLETELY_PROCESSED_OFF:], uint64(len(q.Raw)))
	binary.PutUvarint(buf[_RAW_LENGTH_OFF:], uint64(len(q.Fingerprint)))
	binary.PutUvarint(buf[_TIMESTAMP_OFF:], q.Timestamp)
	binary.PutUvarint(buf[_HASH_OFF:], q.Hash)
	binary.PutUvarint(buf[_FINGERPRINT_HASH_OFF:], q.FingerprintHash)
	binary.PutUvarint(buf[_OFFSET_OFF:], q.Offset)
	binary.PutUvarint(buf[_LENGTH_OFF:], q.Length)

	i := _HEADER_END_OFF
	n := copy(buf[i:], q.Raw)
	i += n
	n = copy(buf[i:], q.Fingerprint)

	return i + n, nil
}

// func UnmarshalBinary(r io.ByteReader) (int, *Query, error) {
// 	q := Query{}

// 	q.CompletelyProcessed = buf[0] == 1

// 	n, err := binary.ReadUvarint(bytes.NewReader(buf[_TIMESTAMP_OFF:]))
// 	if err != nil {
// 		return 0, nil, fmt.Errorf("error reading timestamp: %w", err)
// 	}
// 	q.Timestamp = n

// 	n, err = binary.ReadUvarint(bytes.NewReader(buf[_HASH_OFF:]))
// 	if err != nil {
// 		return 0, nil, fmt.Errorf("error reading hash: %w", err)
// 	}
// 	q.Hash = n

// 	n, err = binary.ReadUvarint(bytes.NewReader(buf[_FINGERPRINT_HASH_OFF:]))
// 	if err != nil {
// 		return 0, nil, fmt.Errorf("error reading fingerprint hash: %w", err)
// 	}
// 	q.FingerprintHash = n

// 	rawLength, err := binary.ReadUvarint(bytes.NewReader(buf[_RAW_LENGTH_OFF:]))
// 	if err != nil {
// 		return 0, nil, fmt.Errorf("error reading raw length: %w", err)
// 	}
// 	fingerprintLength, err := binary.ReadUvarint(bytes.NewReader(buf[_FINGERPRINT_LENGTH_OFF:]))
// 	if err != nil {
// 		return 0, nil, fmt.Errorf("error reading fingerprint length: %w", err)
// 	}

// 	rawOff := _HEADER_END_OFF
// 	if rawLength > 0 {
// 		q.Raw = make([]byte, rawLength)
// 		copy(q.Raw, buf[rawOff:rawOff+int(rawLength)])
// 	}
// 	fingerprintOff := rawOff + int(rawLength)
// 	if fingerprintLength > 0 {
// 		q.Fingerprint = make([]byte, fingerprintLength)
// 		copy(q.Fingerprint, buf[fingerprintOff:fingerprintOff+int(fingerprintLength)])
// 	}

// 	return fingerprintOff + int(fingerprintLength), &q, nil
// }
