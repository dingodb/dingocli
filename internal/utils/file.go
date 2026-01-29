/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package utils

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/schollz/progressbar/v3"
)

type VariantName struct {
	Name                string
	CompressName        string
	LocalCompressName   string
	EncryptCompressName string
}

func RandFilename(dir string) string {
	return fmt.Sprintf("%s/%s", dir, RandString(8))
}

func NewVariantName(name string) VariantName {
	return VariantName{
		Name:                name,
		CompressName:        fmt.Sprintf("%s.tar.gz", name),
		LocalCompressName:   fmt.Sprintf("%s.local.tar.gz", name),
		EncryptCompressName: fmt.Sprintf("%s-encrypted.tar.gz", name),
	}
}

func PathExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func AbsPath(path string) string {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return absPath
}

func GetFilePermissions(path string) int {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}

	return int(info.Mode())
}

func ReadFile(filename string) (string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func WriteFile(filename, data string, mode int) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(mode))
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := file.WriteString(data)
	if err != nil {
		return err
	} else if n != len(data) {
		return fmt.Errorf("write abort")
	}

	return nil
}

func IsFileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	return true
}

func HasExecutePermission(filepath string) (bool, error) {
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return false, err
	}

	return fileInfo.Mode().Perm()&0100 != 0, nil
}

func AddExecutePermission(filepath string) error {
	if ok, err := HasExecutePermission(filepath); err != nil || ok {
		return err
	}

	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return err
	}
	currentMode := fileInfo.Mode()

	newMode := currentMode | 0100

	return os.Chmod(filepath, newMode)
}

func DownloadFileWithProgress(url, destination, filename string) (string, error) {
	// resp, err := http.Get(url)
	// if err != nil {
	// 	return "", err
	// }
	// defer resp.Body.Close()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			TLSHandshakeTimeout:   60 * time.Second,
			ResponseHeaderTimeout: 120 * time.Second,
			ExpectContinueTimeout: 5 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			DisableKeepAlives:     false,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3600*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	tmpFileName := filename
	if tmpFileName == "" {
		tmpFileName = filepath.Base(url)
	}
	tmpFileName = fmt.Sprintf("%s.tmp", tmpFileName)

	if err := os.MkdirAll(destination, 0755); err != nil {
		return "", err
	}

	filePath := filepath.Join(destination, tmpFileName)
	out, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	bar := progressbar.NewOptions64(
		resp.ContentLength,
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]Downloading[reset] %s...", filename)),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(30),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	_, err = io.Copy(io.MultiWriter(out, bar), resp.Body)
	if err != nil {
		os.Remove(filePath)
		return "", err
	}

	if err := os.Rename(filepath.Join(destination, tmpFileName), filepath.Join(destination, filename)); err != nil {
		return "", err
	}

	return filePath, nil
}

func GetRemoteFileContent(url string) (string, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w, url: %s", err, url)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP request failed with status: %s, url: %s", resp.Status, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w, url: %s", err, url)
	}

	return string(body), nil
}
