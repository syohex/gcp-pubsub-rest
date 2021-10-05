package main

import (
	"flag"
	"fmt"
	"os"

	pubsub "github.com/syohex/gcp-pubsub-rest"
)

var subscription string
var accountJSON string
var acknowledge bool

func init() {
	flag.StringVar(&subscription, "sub", "", "subscription name")
	flag.StringVar(&accountJSON, "account", "", "service account JSON path")
	flag.BoolVar(&acknowledge, "ack", false, "send acknowledge after pulling message")
	flag.Parse()
}

func main() {
	os.Exit(_main())
}

func _main() int {
	if subscription == "" || accountJSON == "" {
		fmt.Println("++" + subscription)
		fmt.Println("++" + accountJSON)
		fmt.Printf("Usage: subscriber -sub=sub_name -account=service_account.json")
		return 1
	}

	f, err := os.Open(accountJSON)
	if err != nil {
		fmt.Printf("failed to open %s: %v", os.Args[1], err)
		return 1
	}

	cred, err := pubsub.NewCredential(f)
	if err != nil {
		fmt.Printf("error: %v", err)
		return 1
	}

	res, err := pubsub.PullMessages(cred, subscription, 1, acknowledge)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	for _, msg := range res.ReceivedMessages {
		if msg.Message.Attributes == nil {
			fmt.Printf("MessageID: %s, Data: %s\n", msg.Message.MessageID, msg.Message.Data)
		} else {
			fmt.Printf("MessageID: %s, Data: %s, Attrs: %v\n", msg.Message.MessageID, msg.Message.Data, msg.Message.Attributes)
		}
	}

	return 0
}
