package main

import (
	"log"

	"github.com/aws/aws-lambda-go/lambda"
)

type MyEvent struct {
	Name string `json:"What is your name?"`
	Age  int    `json:"How old are you?"`
}

type MyResponse struct {
	Message string `json:"Answer:"`
}

func HandleLambdaEvent(event string) (MyResponse, error) {
	log.Print(event)
	return MyResponse{Message: event}, nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
