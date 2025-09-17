package main

import (
	"bufio"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"sync"
)

// Source: https://github.com/golang/go/blob/go1.20.5/src/net/dnsclient.go#L72-L75
func isValidDomain(s string) bool {
	// See RFC 1035, RFC 3696.
	// Presentation format has dots before every label except the first, and the
	// terminal empty label is optional here because we assume fully-qualified
	// (absolute) input. We must therefore reserve space for the first and last
	// labels' length octets in wire format, where they are necessary and the
	// maximum total length is 255.
	// So our _effective_ maximum is 253, but 254 is not rejected if the last
	// character is a dot.
	l := len(s)
	if l == 0 || l > 254 || l == 254 && s[l-1] != '.' {
		return false
	}

	last := byte('.')
	nonNumeric := false // true once we've seen a letter or hyphen
	partlen := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		default:
			return false
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '_':
			nonNumeric = true
			partlen++
		case '0' <= c && c <= '9':
			// fine
			partlen++
		case c == '-':
			// Byte before dash cannot be dot.
			if last == '.' {
				return false
			}
			partlen++
			nonNumeric = true
		case c == '.':
			// Byte before dot cannot be dot, dash.
			if last == '.' || last == '-' {
				return false
			}
			if partlen > 63 || partlen == 0 {
				return false
			}
			partlen = 0
		}
		last = c
	}
	if last == '-' || partlen > 63 {
		return false
	}

	return nonNumeric
}

func stripDot(domainName string) string {
	dlen := len(domainName) - 1
	if dlen-1 <= 0 || domainName[dlen:] != "." {
		return domainName
	}

	return domainName[:dlen]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type ValidationResult struct {
	isValid bool
	output  string
}

func chOutputWriter(outputCh *chan ValidationResult) {
	for {
		result, ok := <-*outputCh
		if !ok {
			break
		}

		if !result.isValid {
			fmt.Fprintln(os.Stderr, result.output)
			continue
		}

		fmt.Fprintln(os.Stdout, result.output)
	}
}

func chInputValidator(validationCh *chan string, validationFn func(string) ValidationResult,
	outputCh *chan ValidationResult) {
	for {
		line, ok := <-*validationCh
		if !ok {
			break
		}

		result := validationFn(line)
		*outputCh <- result
	}
}

func cleanAndOutputLine(validatorWorkerCount int, validationFn func(string) ValidationResult) {
	if validatorWorkerCount <= 0 {
		validatorWorkerCount = 50
	}

	scanner := bufio.NewScanner(os.Stdin)
	validationCh := make(chan string, 550_000)
	outputCh := make(chan ValidationResult, 550_000)
	var validatorWg sync.WaitGroup
	var writerWg sync.WaitGroup

	// start validators
	for range validatorWorkerCount {
		validatorWg.Add(1)

		go func() {
			chInputValidator(&validationCh, validationFn, &outputCh)
			defer validatorWg.Done()
		}()
	}

	writerWg.Add(1)
	go func() {
		chOutputWriter(&outputCh)
		defer writerWg.Done()
	}()

	for scanner.Scan() {
		line := scanner.Text()
		validationCh <- line
	}

	close(validationCh)
	validatorWg.Wait()

	close(outputCh)
	writerWg.Wait()

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading from stdin:", err)
	}
}

var TLDs = map[string]string{}

//go:embed TLD.txt
var tldFile string

func init() {
	for _, tld := range strings.Split(tldFile, "\n") {
		TLDs[tld] = tld
	}
}

func getApexDomain(domain string) string {
	// Split domain into parts using '.' as the separator
	parts := strings.Split(domain, ".")
	tldIndex := len(parts) - 1
	l2TldIndex := tldIndex
	labelIndex := 0
	tldExists := tldIndex > 0

	if !tldExists {
		return domain
	}

	// Output
	var apexDomain strings.Builder

	// If level 2 TLD exists
	if _, exists := TLDs[parts[tldIndex-1]]; exists {
		l2TldIndex = tldIndex - 1
	}

	labelIndex = max(min(tldIndex, l2TldIndex)-1, 0)

	// Extract ApexDomain
	apexParts := parts[labelIndex : tldIndex+1]
	apexDomain.WriteString(strings.Join(apexParts, "."))

	return apexDomain.String()
}

func AttemptDomainCleanUp(domain string) (string, error) {
	if len(domain) <= 2 {
		return domain, ErrorDomainTooShort
	}

	if domain[:2] == `\"` || domain[:2] == `*.` {
		cleanDom := domain[2:]
		if isValidDomain(cleanDom) {
			return cleanDom, nil
		}
	}

	return domain, ErrorDomainCleanUpFailed
}
