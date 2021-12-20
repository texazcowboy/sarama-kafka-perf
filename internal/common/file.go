package common

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func WriteLinesToFile(path string, lines ...string) {
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("unable to create output file: %s. error: %s", path, err.Error())
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Fatalf("unable to close file: %s. error: %s", path, err.Error())
		}
	}()
	bufWriter := bufio.NewWriter(file)
	for _, line := range lines {
		if _, err := fmt.Fprintln(bufWriter, line); err != nil {
			log.Fatalf("unable to write lines to buffer: %s. error: %s", line, err.Error())
		}
	}
	if err := bufWriter.Flush(); err != nil {
		log.Fatalf("unable to flush lines to file: %s. error: %s", path, err.Error())
	}
}

func ReadLinesFromFile(path string) []string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("unable to open input file: %s. error: %s", path, err.Error())
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Fatalf("unable to close file: %s. error: %s", path, err.Error())
		}
	}()

	bufScanner := bufio.NewScanner(file)

	var lines []string
	for bufScanner.Scan() {
		lines = append(lines, bufScanner.Text())
	}

	return lines
}