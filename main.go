package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
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

type Config struct {
	Key   string   `json:"key"`
	Split []string `json:"split"`
}

func HandleLambdaEvent(event EvEnt) (string, error) {
	if len(event.Records) < 1 {
		return "", nil
	}
	Bucket := ""
	jsonFile := ""
	Record := event.Records[0]
	Bucket = Record.S3.Bucket.Name
	jsonFile = Record.S3.Object.Key
	if Bucket != "" && jsonFile != "" {
		return "", nil
	}
	svc := s3.New(session.New())
	input := &s3.GetObjectInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(jsonFile),
	}
	result, err := svc.GetObject(input)
	if err != nil {
		return "", err
	}
	log.Println(result.String())
	return result.String(), err
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
