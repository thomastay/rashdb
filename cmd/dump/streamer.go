package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Streams data in JSON5 format (allows trailing commas)
// The purpose is to allow dumping of data to the console even if the DB file is really big
// without holding everything in memory (DB files can be gigabytes large!)
type Streamer struct {
	*bufio.Writer
	indent int
}

func NewStreamer(w io.Writer) *Streamer {
	return &Streamer{Writer: bufio.NewWriter(w)}
}

func (s *Streamer) StreamKV(key string, val interface{}) error {
	valBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	buf := fmt.Sprintf("%s\"%s\": %s,\n", strings.Repeat(" ", s.indent), key, valBytes)
	s.Write([]byte(buf))
	return nil
}

func (s *Streamer) StreamStr(str string) error {
	buf := fmt.Sprintf("%s%s\n", strings.Repeat(" ", s.indent), str)
	s.Write([]byte(buf))
	return nil
}

func (s *Streamer) StreamObjOpen(str string) error {
	var buf string
	if str == "" {
		buf = fmt.Sprintf("%s{\n", strings.Repeat(" ", s.indent))
	} else {
		buf = fmt.Sprintf("%s\"%s\": {\n", strings.Repeat(" ", s.indent), str)
	}

	s.Write([]byte(buf))
	s.indent += 2

	return nil
}

func (s *Streamer) StreamObjClose(trailing bool) error {
	s.indent -= 2
	if s.indent < 0 {
		return errors.New("Close called before Open")
	}
	buf := fmt.Sprintf("%s},\n", strings.Repeat(" ", s.indent))
	if !trailing {
		buf = fmt.Sprintf("%s}\n", strings.Repeat(" ", s.indent))
	}
	s.Write([]byte(buf))
	return nil
}

func (s *Streamer) StreamArrOpen(str string) error {
	buf := fmt.Sprintf("%s\"%s\": [\n", strings.Repeat(" ", s.indent), str)
	s.Write([]byte(buf))
	s.indent += 2

	return nil
}

func (s *Streamer) StreamArrClose() error {
	s.indent -= 2
	if s.indent < 0 {
		return errors.New("Close called before Open")
	}
	buf := fmt.Sprintf("%s],\n", strings.Repeat(" ", s.indent))
	s.Write([]byte(buf))
	return nil
}
