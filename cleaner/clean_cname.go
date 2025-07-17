package main

import (
	_ "embed"
	"fmt"
	"strings"
)

func validateAndReturnCNAME(line string) ValidationResult {
	parts := strings.Split(line, ",")
	partsLen := len(parts)

	if partsLen != 3 {
		return ValidationResult{
			isValid: false,
			output:  fmt.Sprintf("EL,%s", line),
		}
	}

	if !isValidDomain(parts[0]) || !isValidDomain(parts[1]) || !isValidDomain(parts[2]) {
		return ValidationResult{
			isValid: false,
			output:  fmt.Sprintf("ED,%s", line),
		}
	}

	return ValidationResult{
		isValid: true,
		output:  fmt.Sprintf("%s,%s", parts[0], parts[2]),
	}
}

func cleanCNAME() {
	cleanAndOutputLine(500, validateAndReturnCNAME)
}
