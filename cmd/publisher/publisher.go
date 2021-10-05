package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	pubsub "github.com/syohex/gcp-pubsub-rest"
)

var topicName string
var accountJSON string
var attributes string

func init() {
	flag.StringVar(&topicName, "topic", "", "topic name")
	flag.StringVar(&accountJSON, "account", "", "service account JSON path")
	flag.StringVar(&attributes, "attr", "", "message attributes(map[string]string)")
	flag.Parse()
}

func main() {
	os.Exit(_main())
}

func _main() int {
	if len(flag.Args()) == 0 || topicName == "" || accountJSON == "" {
		fmt.Printf("Usage: publisher -topic=topic_name -account=service_account.json message")
		return 1
	}

	f, err := os.Open(accountJSON)
	if err != nil {
		fmt.Printf("failed to open %s: %v", accountJSON, err)
		return 1
	}

	cred, err := pubsub.NewCredential(f)
	if err != nil {
		fmt.Printf("error: %v", err)
		return 1
	}

	var attrs map[string]string
	if attributes != "" {
		tmp := make(map[string]string)
		if err := json.Unmarshal([]byte(attributes), &tmp); err == nil {
			attrs = tmp
		}
	}

	res, err := pubsub.PublishString(cred, "test_topic", flag.Arg(0), attrs)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	fmt.Printf("send message ID=%s\n", res.MessageIDs[0])
	return 0
}
