package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/knadh/listmonk/internal/media/providers/filesystem"
	"github.com/knadh/listmonk/internal/media/providers/s3"
)

var (
	regexpSpaces = regexp.MustCompile(`[\s]+`)
)

// inArray checks if a string is present in a list of strings.
func inArray(val string, vals []string) (ok bool) {
	for _, v := range vals {
		if v == val {
			return true
		}
	}
	return false
}

// makeFilename sanitizes a filename (user supplied upload filenames).
func makeFilename(fName string) string {
	name := strings.TrimSpace(fName)
	if name == "" {
		name, _ = generateRandomString(10)
	}
	// replace whitespace with "-"
	name = regexpSpaces.ReplaceAllString(name, "-")
	return filepath.Base(name)
}

// appendSuffixToFilename adds a string suffix to the filename while keeping the file extension.
func appendSuffixToFilename(filename, suffix string) string {
	ext := filepath.Ext(filename)
	name := strings.TrimSuffix(filename, ext)
	return fmt.Sprintf("%s_%s%s", name, suffix, ext)
}

// makeMsgTpl takes a page title, heading, and message and returns
// a msgTpl that can be rendered as an HTML view. This is used for
// rendering arbitrary HTML views with error and success messages.
func makeMsgTpl(pageTitle, heading, msg string) msgTpl {
	if heading == "" {
		heading = pageTitle
	}
	err := msgTpl{}
	err.Title = pageTitle
	err.MessageTitle = heading
	err.Message = msg
	return err
}

// parseStringIDs takes a slice of numeric string IDs and
// parses each number into an int64 and returns a slice of the
// resultant values.
func parseStringIDs(s []string) ([]int, error) {
	vals := make([]int, 0, len(s))
	for _, v := range s {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}

		if i < 1 {
			return nil, fmt.Errorf("%d is not a valid ID", i)
		}

		vals = append(vals, i)
	}

	return vals, nil
}

// generateRandomString generates a cryptographically random, alphanumeric string of length n.
func generateRandomString(n int) (string, error) {
	const dictionary = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)

	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}

	return string(bytes), nil
}

// strHasLen checks if the given string has a length within min-max.
func strHasLen(str string, min, max int) bool {
	return len(str) >= min && len(str) <= max
}

// strSliceContains checks if a string is present in the string slice.
func strSliceContains(str string, sl []string) bool {
	for _, s := range sl {
		if s == str {
			return true
		}
	}

	return false
}

func trimNullBytes(b []byte) string {
	return string(bytes.Trim(b, "\x00"))
}

func titleCase(input string) string {
	parts := strings.Fields(input)
	for n, p := range parts {
		parts[n] = strings.Title(p)
	}

	return strings.Join(parts, " ")
}

func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func parseS3Opts(data map[string]interface{}) s3.Opt {
	expiryStr, ok := data["expiry"].(string)
	if !ok {
		lo.Fatalf("invalid type for expiry: expected string, got %T", data["expiry"])
	}
	expiryStr = strings.Trim(expiryStr, "\"")
	expiry, err := time.ParseDuration(expiryStr)
	if err != nil {
		lo.Fatalf("error parsing expiry duration: %s", err)
	}

	return s3.Opt{
		URL:        data["url"].(string),
		PublicURL:  data["public_url"].(string),
		AccessKey:  data["aws_access_key_id"].(string),
		SecretKey:  data["aws_secret_access_key"].(string),
		Region:     data["aws_default_region"].(string),
		Bucket:     data["bucket"].(string),
		BucketPath: data["bucket_path"].(string),
		BucketType: data["bucket_type"].(string),
		Expiry:     expiry,
	}
}
func parseFileSystemOpts(data map[string]interface{}) filesystem.Opts {
	return filesystem.Opts{
		UploadPath: data["upload_path"].(string),
		UploadURI:  data["upload_uri"].(string),
		RootURL:    data["root_url"].(string),
	}
}
