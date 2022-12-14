package main

import (
	"context"
	"encoding/json"
	runtime "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"log"
	"os"
)

type EvEnt struct {
	Records []struct {
		AwsRegion   string `json:"awsRegion"`
		EventName   string `json:"eventName"`
		EventSource string `json:"eventSource"`
		EventTime   struct {
			IMillis     int64 `json:"iMillis"`
			IChronology struct {
				IBase struct {
					IMinDaysInFirstWeek int `json:"iMinDaysInFirstWeek"`
				} `json:"iBase"`
			} `json:"iChronology"`
		} `json:"eventTime"`
		EventVersion      string `json:"eventVersion"`
		RequestParameters struct {
			SourceIPAddress string `json:"sourceIPAddress"`
		} `json:"requestParameters"`
		ResponseElements struct {
			XAmzID2       string `json:"xAmzId2"`
			XAmzRequestID string `json:"xAmzRequestId"`
		} `json:"responseElements"`
		S3 struct {
			ConfigurationID string `json:"configurationId"`
			Bucket          struct {
				Name          string `json:"name"`
				OwnerIdentity struct {
					PrincipalID string `json:"principalId"`
				} `json:"ownerIdentity"`
				Arn string `json:"arn"`
			} `json:"bucket"`
			Object struct {
				Key       string `json:"key"`
				Size      int    `json:"size"`
				ETag      string `json:"eTag"`
				VersionID string `json:"versionId"`
				Sequencer string `json:"sequencer"`
			} `json:"object"`
			S3SchemaVersion string `json:"s3SchemaVersion"`
		} `json:"s3"`
		UserIdentity struct {
			PrincipalID string `json:"principalId"`
		} `json:"userIdentity"`
	} `json:"records"`
}

var client = lambda.New(session.New())

func callLambda() (string, error) {
	input := &lambda.GetAccountSettingsInput{}
	req, resp := client.GetAccountSettingsRequest(input)
	err := req.Send()
	output, _ := json.Marshal(resp.AccountUsage)
	return string(output), err
}

func handleRequest(ctx context.Context, event EvEnt) (string, error) {
	// event
	eventJson, _ := json.MarshalIndent(event, "", "  ")
	log.Printf("EVENT: %s", eventJson)
	// environment variables
	log.Printf("REGION: %s", os.Getenv("AWS_REGION"))
	log.Println("ALL ENV VARS:")
	for _, element := range os.Environ() {
		log.Println(element)
	}
	// request context
	lc, _ := lambdacontext.FromContext(ctx)
	log.Printf("REQUEST ID: %s", lc.AwsRequestID)
	// global variable
	log.Printf("FUNCTION NAME: %s", lambdacontext.FunctionName)
	// context method
	deadline, _ := ctx.Deadline()
	log.Printf("DEADLINE: %s", deadline)
	// AWS SDK call
	usage, err := callLambda()
	if err != nil {
		return "ERROR", err
	}
	return usage, nil
}

func main() {
	runtime.Start(handleRequest)
}
