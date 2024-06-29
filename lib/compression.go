package lib

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zip"
)

func Compress(data []byte) ([]byte, error) {
	var compressed bytes.Buffer
	compressor := gzip.NewWriter(&compressed)
	// Compress the string
	_, err := compressor.Write(data)
	if err != nil {
		fmt.Println("Error compressing string:", err)
		return nil, err
	}
	compressor.Close()
	return compressed.Bytes(), nil
}

func Decompress(data []byte) ([]byte, error) {
	// Decompress and print the original string
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		fmt.Println("Error reading string:", err)
		return nil, err
	}
	d, err := io.ReadAll(r)
	if err != nil {
		fmt.Println("Error decompressing string:", err)
		return nil, err
	}
	r.Close()
	return d, nil
}

func CompressFolder(source, target string) error {
	targetFile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer targetFile.Close()

	zipWriter := zip.NewWriter(targetFile)
	defer zipWriter.Close()

	fileInfo, err := os.Stat(source)
	if err != nil {
		return err
	}

	var baseDir string
	if fileInfo.IsDir() {
		baseDir = filepath.Base(source)
	}
	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		// Create a new zip file entry
		filePath := path
		if baseDir != "" {
			filePath = filepath.Join(baseDir, strings.TrimPrefix(path, source))
		}
		zipEntry, err := zipWriter.Create(filePath)
		if err != nil {
			return err
		}

		// Copy file contents to zip entry
		_, err = io.Copy(zipEntry, file)
		return err
	})
}

/*
func decompressFolder(source, target string) error {
	zipReader, err := zip.OpenReader(source)
	if err != nil {
		return err
	}
	defer zipReader.Close()

	for _, file := range zipReader.File {
		filePath := filepath.Join(target, file.Name)
		if file.FileInfo().IsDir() {
			os.MkdirAll(filePath, os.ModePerm)
			continue
		}

		err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
		if err != nil {
			return err
		}

		outFile, err := os.Create(filePath)
		if err != nil {
			return err
		}
		defer outFile.Close()

		zipFile, err := file.Open()
		if err != nil {
			return err
		}
		defer zipFile.Close()

		_, err = io.Copy(outFile, zipFile)
		if err != nil {
			return err
		}
	}
	return nil
}

func readFilesFromFolder(folder string) ([]os.FileInfo, error) {
	var files []os.FileInfo
	dirEntries, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}
	for _, entry := range dirEntries {
		if !entry.IsDir() {
			inf, _ := entry.Info()
			files = append(files, inf)
		}
	}
	return files, nil
}
*/
