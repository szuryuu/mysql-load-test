package main

import (
	stdbytes "bytes"

	"github.com/bagaswh/mysql-toolkit/pkg/bytes"
)

var whitespaces = [256]bool{
	' ':  true,
	'\t': true,
	'\n': true,
	'\r': true,
}

func isWhitespace(b byte) bool {
	return whitespaces[b]
}

func bytesContainsWeirdChars(b []byte) bool {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return true
		}
	}
	return false
}

func bytesTrimSpace(b []byte) []byte {
	return bytesTrimFuncInPlace(b, isWhitespace)
}

func bytesTrimFuncInPlace(b []byte, fn func(byte) bool) []byte {
	i := 0
	bi := 0
	trailingTrimAt := -1
	for i < len(b) {
		if bi == 0 && fn(b[i]) {
			i++
			continue
		}
		b[bi] = b[i]
		if fn(b[bi]) {
			if trailingTrimAt == -1 {
				trailingTrimAt = bi
			}
		} else {
			trailingTrimAt = -1
		}
		bi++
		i++
	}

	if trailingTrimAt != -1 {
		return b[:trailingTrimAt:trailingTrimAt]
	}
	return b[:bi:bi]
}

var (
	weirdSequence1 = []byte{0x9, 0x9, 0x9, 0x9}
)

func isValidQuery(q []byte) bool {
	if len(q) == 0 {
		return false
	}

	if bytesContainsWeirdChars(q[:min(len(q), 25)]) {
		return false
	}

	bytes.ToLowerInPlace(q)
	if stdbytes.HasPrefix(q, []byte{'u', 's', 'e', ' '}) ||
		stdbytes.HasPrefix(q, []byte{'s', 'e', 't', ' '}) {
		return false
	}

	return true
}

var invalidFingerprintPrefixes = [][]byte{
	// this contains multiple select somehow
	[]byte("select ticket_status, chat_log_id_start, chat_log_id_end from botika_helpdesk_tickets where bot_id = ? and ticket_status != ? and ticket_status != ? and ticket_group = ? select ticket_status, chat_log_id_start, chat_log_id_end from botika_helpdesk_tickets where bot_id = ? and ticket_status != ? and ticket_status != ? and user_id = ? order by ticket_idx desc limit ?"),
	[]byte("select ticket_status, ticket_idx, creation_date, chat_log_id_start, chat_log_idx_start, chat_log_id_end from botika_helpdesk_tickets where bot_id = ? and ticket_status != ? and ticket_status != ? and ticket_group = ? select ticket_status, ticket_idx, creation_date, chat_log_id_start, chat_log_idx_start, chat_log_id_end from botika_helpdesk_tickets where bot_id = ? and ticket_status != ? and ticket_status != ? and user_id = ? order by ticket_idx desc limit ?"),
	[]byte("select * from rule_state"),
	[]byte("select * from rule_action"),
	[]byte("select * from botika_push_messages"),
	[]byte("update botika_push_messages"),
	[]byte("select * from botika_tts_history"),
	[]byte("update botika_tts_history"),
	[]byte("select count(*) from botika_notification_gallery"),
	[]byte("update botika_voicebotstream_limit"),
}

func isValidFingerprint(q []byte) bool {
	for _, invalid := range invalidFingerprintPrefixes {
		if stdbytes.HasPrefix(q, invalid) {
			return false
		}
	}
	return true
}
