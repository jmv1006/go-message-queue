# Go Message Queue

Go-Message-Queue is an event streaming platform inspired by Kafka. It listens on a user defined TCP port. 

Incoming messages should be sent as a BASE64 encoded valid JSON string, and in the following format:

```
{
type: "",
body: ""
}
```

`type` must be either __PRODUCE__ or __CONSUME__, depending on the type of connection that
the client is intending to make.

A __PRODUCE__ request will simply write the `body` as a message to the queue,
while a __CONSUME__ request will consume any new messages written to the queue
by other clients.

## Environment Variables:
- LISTENER_ADDRESS (required): The full address & port where the program will listen for incoming requests