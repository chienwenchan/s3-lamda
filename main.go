package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

const Verion = "1.0"

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
	Key   string `json:"key"`
	Split []struct {
		Key  string `json:"key"`
		Size int    `json:"size"`
	} `json:"split"`
}

func GetLocalTimeZone() *time.Location {
	return time.FixedZone("CST", 8*3600) // UTC+8
}

func HandleLambdaEvent(ctx context.Context, event EvEnt) (string, error) {
	eventJson, _ := json.MarshalIndent(event, "", "  ")
	log.Printf("EVENT: %s", eventJson)
	lc, _ := lambdacontext.FromContext(ctx)
	log.Printf("REQUEST ID: %s", lc.AwsRequestID)
	log.Printf("FUNCTION NAME: %s", lambdacontext.FunctionName)
	now := time.Now().In(GetLocalTimeZone()).Unix()
	if len(event.Records) < 1 {
		return "", nil
	}
	Bucket := ""
	jsonFile := ""
	Record := event.Records[0]
	Bucket = Record.S3.Bucket.Name
	jsonFile = Record.S3.Object.Key
	if Bucket == "" && jsonFile == "" {
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
	log.Printf("下载配置文件开始")
	ret, _ := ioutil.ReadAll(result.Body)
	defer result.Body.Close()
	config := Config{}
	err = json.Unmarshal(ret, &config)
	if err != nil {
		return "", err
	}
	log.Printf("下载配置文件结束")
	h := md5.New()
	h.Write([]byte(config.Key))
	basePath := "/tmp/" + hex.EncodeToString(h.Sum(nil))
	err = os.MkdirAll(basePath, 0755)
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(basePath)
	tmp := strings.Split(config.Key, "/")
	out, err := os.Create(basePath + "/" + tmp[len(tmp)-1])
	if err != nil {
		return "", err
	}
	writer := bufio.NewWriter(out)
	log.Printf("开始下载分片文件")
	for _, s := range config.Split {
		splitInput := &s3.GetObjectInput{
			Bucket: aws.String(Bucket),
			Key:    aws.String(s.Key),
		}
		split, err := svc.GetObject(splitInput)
		if err != nil {
			return "", err
		}
		splitBody, _ := ioutil.ReadAll(split.Body)
		writer.Write(splitBody)
		split.Body.Close()
	}
	writer.Flush()
	out.Close()
	log.Printf("下载分片文件结束")
	now = time.Now().In(GetLocalTimeZone()).Unix()
	fileData, _ := os.Open(basePath + "/" + tmp[len(tmp)-1])
	svc.PutObject(&s3.PutObjectInput{
		Body:   fileData,
		Bucket: aws.String(Bucket),
		Key:    aws.String(config.Key),
	})
	fileData.Close()
	log.Printf("version:%s:---:总耗时:%d", Verion, time.Now().In(GetLocalTimeZone()).Unix()-now)
	return result.String(), err
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
