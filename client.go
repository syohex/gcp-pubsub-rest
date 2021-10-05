package pubsub

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const baseUrl = "https://pubsub.googleapis.com/v1"

type pubsubMessages struct {
	Messages []pubsubMessage `json:"messages"`
}

type pubsubMessage struct {
	Data        string            `json:"data"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	MessageID   string            `json:"messageId,omitempty"`
	PublishTime string            `json:"publishTime,omitempty"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

type PublishResponse struct {
	MessageIDs []string `json:"messageIds"`
}

type pubsubPullRequest struct {
	MaxMessages int `json:"maxMessages"`
}

type PullResponse struct {
	ReceivedMessages []pubsubReceivedMessage `json:"receivedMessages"`
}

type pubsubReceivedMessage struct {
	AckID           string        `json:"ackId"`
	Message         pubsubMessage `json:"message"`
	DeliveryAttempt int           `json:"deliveryAttempt"`
}

type acknowledgeRequest struct {
	AckIDs []string `json:"ackIds"`
}

func PublishString(cred *Credential, topic string, str string, attrs map[string]string) (*PublishResponse, error) {
	token, err := getAccessToken(cred)
	if err != nil {
		return nil, err
	}

	endPoint := fmt.Sprintf("%s/projects/%s/topics/%s:publish", baseUrl, cred.ProjectID, topic)
	msg := base64.StdEncoding.EncodeToString([]byte(str))

	messages := pubsubMessages{
		Messages: []pubsubMessage{
			{
				Data:       msg,
				Attributes: attrs,
			},
		},
	}

	bs, err := json.Marshal(&messages)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(bs)

	client := &http.Client{}
	req, err := http.NewRequest("POST", endPoint, r)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json; charset=utf-8")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var pubRes PublishResponse
	if err := json.NewDecoder(res.Body).Decode(&pubRes); err != nil {
		return nil, err
	}

	return &pubRes, nil
}

func PullMessages(cred *Credential, subscription string, count int, acknowledge bool) (*PullResponse, error) {
	token, err := getAccessToken(cred)
	if err != nil {
		return nil, err
	}

	endPoint := fmt.Sprintf("%s/projects/%s/subscriptions/%s:pull", baseUrl, cred.ProjectID, subscription)

	pubsubReq := pubsubPullRequest{
		MaxMessages: count,
	}

	bs, err := json.Marshal(&pubsubReq)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(bs)

	client := &http.Client{}
	req, err := http.NewRequest("POST", endPoint, r)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json; charset=utf-8")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("got error response")
	}

	var pullRes PullResponse
	if err := json.NewDecoder(res.Body).Decode(&pullRes); err != nil {
		return nil, err
	}

	var ackIDs []string
	for i := range pullRes.ReceivedMessages {
		ackIDs = append(ackIDs, pullRes.ReceivedMessages[i].AckID)

		b, err := base64.StdEncoding.DecodeString(pullRes.ReceivedMessages[i].Message.Data)
		if err != nil {
			return nil, err
		}

		pullRes.ReceivedMessages[i].Message.Data = string(b)
	}

	if acknowledge {
		if err := sendAcknowledge(cred.ProjectID, subscription, token, ackIDs); err != nil {
			return nil, err
		}
	}

	return &pullRes, nil
}

func sendAcknowledge(projectID string, subscription string, accessToken string, ackIDs []string) error {
	endPoint := fmt.Sprintf("%s/projects/%s/subscriptions/%s:acknowledge", baseUrl, projectID, subscription)

	ackReq := acknowledgeRequest{
		ackIDs,
	}

	bs, err := json.Marshal(&ackReq)
	if err != nil {
		return err
	}
	r := bytes.NewReader(bs)

	client := &http.Client{}
	req, err := http.NewRequest("POST", endPoint, r)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json; charset=utf-8")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		bs, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("got error response: %s", string(bs))
	}

	return nil
}