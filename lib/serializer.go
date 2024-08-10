package lib

import (
	"bytes"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// Serialize encodes the given value into MessagePack bytes.
func Serialize[T any](value T) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize decodes the given MessagePack bytes into a value.
func Deserialize[T any](data []byte) (T, error) {
	var value T
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&value); err != nil {
		return value, err
	}
	return value, nil
}

// SerializeStream encodes the given value and writes it to the provided writer in MessagePack format.
func SerializeStream[T any](writer io.Writer, value T) error {
	enc := msgpack.NewEncoder(writer)
	return enc.Encode(value)
}

// DeserializeStream reads and decodes a value from the provided reader in MessagePack format.
func DeserializeStream[T any](reader io.Reader) (T, error) {
	var value T
	dec := msgpack.NewDecoder(reader)
	if err := dec.Decode(&value); err != nil {
		return value, err
	}
	return value, nil
}
