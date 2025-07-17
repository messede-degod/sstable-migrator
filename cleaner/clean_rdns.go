package main

import (
	"fmt"
	"strings"
)

func validateAndReturnRDNS(line string) ValidationResult {
	parts := strings.Split(line, ",")

	parts[1] = stripDot(parts[1])
	parts[2] = stripDot(parts[2])

	if len(parts) != 4 {
		return ValidationResult{
			isValid: false,
			output:  fmt.Sprintf("EL,%s", line),
		}
	}

	if !isValidDomain(parts[2]) {
		return ValidationResult{
			isValid: false,
			output:  fmt.Sprintf("ED,%s", line),
		}
	}

	calculatedApex := stripDot(getApexDomain(parts[2]))
	if calculatedApex != parts[1] {
		return ValidationResult{
			isValid: false,
			output:  fmt.Sprintf("EA,%s", line),
		}
	}

	return ValidationResult{
		isValid: true,
		output:  fmt.Sprintf("%s,%s,%s", parts[0], parts[2], parts[3]),
	}
}

func cleanRDNS() {
	cleanAndOutputLine(500, validateAndReturnRDNS)
}
