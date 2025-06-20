package main

import (
	"bytes"
	"testing"
)

func Test_BytesTrimSpaceInPlace(t *testing.T) {
	tests := []struct {
		name string
		b    []byte
		want []byte
	}{
		{
			name: "trim space",
			b:    []byte("  select * from test  "),
			want: []byte("select * from test"),
		},
		// {
		// 	name: "no trim",
		// 	b:    []byte("select * from test"),
		// 	want: []byte("select * from test"),
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newb := bytesTrimSpaceInPlace(tt.b)
			if !bytes.Equal(newb, tt.want) {
				t.Errorf("bytesTrimSpaceInPlace()\n\t% x,\n\t% x\n", string(tt.b), string(tt.want))
			}
			if cap(newb) != cap(tt.b) {
				t.Errorf("bytesTrimSpaceInPlace() cap = %d, want %d", cap(newb), cap(tt.want))
			}
		})
	}
}
