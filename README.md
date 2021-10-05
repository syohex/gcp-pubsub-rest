# GCP pub/sub sample using REST API in Go

GCP pub/sub publisher and subscriber sample programs. These use REST API and don't use pub/sub client library

## Publish

```bash
cd cmd/publisher
go run publisher.go \
  -topic=topic_name \
  -account=your_service_account_json_path \
  -attr='{"name": "Tom"}'
  'hello world'
```

## Subscribe

```bash
cd cmd/subscriber
go run subscriber.go \
  -sub=subscription_name \
  -account=your_service_account_json_path \
  -ack
```
