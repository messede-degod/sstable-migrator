package main

import (
	_ "embed"
	"fmt"
	"strings"
)

func validateAndReturnSubs(line string) ValidationResult {
	parts := strings.Split(line, ",")
	partsLen := len(parts)

	if partsLen != 8 {
		return ValidationResult{
			isValid: false,
			output:  fmt.Sprintf("EL,%s", line),
		}
	}

	lastSeen := parts[partsLen-1]
	domainParts := []string{}
	for i := partsLen - 2; i >= 0; i-- {
		part := stripDot(parts[i])
		if part == "" || part == "*" {
			continue
		}
		domainParts = append(domainParts, part)
	}

	domain := strings.Join(domainParts, ".")

	if !isValidDomain(domain) {
		return ValidationResult{
			isValid: false,
			output:  fmt.Sprintf("ED,%s,%s", domain, lastSeen),
		}
	}

	return ValidationResult{
		isValid: true,
		output:  fmt.Sprintf("%s,%s", domain, lastSeen),
	}
}

func cleanSubs() {
	cleanAndOutputLine(500, validateAndReturnSubs)
}
