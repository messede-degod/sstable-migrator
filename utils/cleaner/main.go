package main

import (
	"flag"
	"fmt"
)

func main() {
	rdnsFlag := flag.Bool("rdns", false, "Clean RDNS")
	subsFlag := flag.Bool("subs", false, "Clean SubDomains")
	CNAMEFlag := flag.Bool("cname", false, "Clean CNAMEs")
	flag.Parse()

	switch {
	case *rdnsFlag && *subsFlag:
		fmt.Println("Both -rdns and -subs flags are set. Please choose one.")
	case *rdnsFlag:
		cleanRDNS()
	case *subsFlag:
		cleanSubs()
	case *CNAMEFlag:
		cleanCNAME()
	default:
		fmt.Println("Input is read from STDIN, Output is sent to STDOUT and STDERR")
		fmt.Println("No flags provided. Available flags:")
		flag.PrintDefaults()
	}
}
