package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"time"
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

func GetLocalTimeZone() *time.Location {
	return time.FixedZone("CST", 8*3600) // UTC+8
}

func HandleLambdaEvent(event EvEnt) (string, error) {
	log.Println("start", time.Now().In(GetLocalTimeZone()).Unix())
	if len(event.Records) < 1 {
		return "", nil
	}
	log.Println("step 1:", time.Now().In(GetLocalTimeZone()).Unix())
	Bucket := ""
	jsonFile := ""
	Record := event.Records[0]
	Bucket = Record.S3.Bucket.Name
	jsonFile = Record.S3.Object.Key
	if Bucket == "" && jsonFile == "" {
		return "", nil
	}
	log.Println("step 2:", time.Now().In(GetLocalTimeZone()).Unix())
	svc := s3.New(session.New())
	input := &s3.GetObjectInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(jsonFile),
	}
	log.Println("step 3:", time.Now().In(GetLocalTimeZone()).Unix())
	result, err := svc.GetObject(input)
	if err != nil {
		return "", err
	}
	log.Println("step 4:", time.Now().In(GetLocalTimeZone()).Unix())
	log.Println(result.String())
	log.Println("step 5:", time.Now().In(GetLocalTimeZone()).Unix())
	return result.String(), err
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
