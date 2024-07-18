package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func reverseDomain(input string) string {
	// Split the input string by commas
	components := strings.Split(input, ",")

	// Reverse the components array
	for i, j := 0, len(components)-1; i < j; i, j = i+1, j-1 {
		components[i], components[j] = components[j], components[i]
	}

	// Build the full domain using a strings.Builder
	var builder strings.Builder
	for _, component := range components {
		builder.WriteString(component)
		builder.WriteString(".")
	}
	fullDomain := builder.String()
	return strings.TrimSuffix(fullDomain, ".")
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Please provide input and output file names")
		return
	}

	inputFile, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println("Error opening input file:", err)
		return
	}
	defer inputFile.Close()

	outputFile, err := os.Create(os.Args[2])
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	writer := bufio.NewWriter(outputFile)

	for scanner.Scan() {
		input := scanner.Text()
		fullDomain := reverseDomain(input)
		writer.WriteString(fullDomain + "\n")
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input file:", err)
	}

	writer.Flush()
}
