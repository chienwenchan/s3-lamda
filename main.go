package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"os"
	"strings"
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
	Key   string `json:"key"`
	Split []struct {
		Key  string `json:"key"`
		Size int    `json:"size"`
	} `json:"split"`
}

func GetLocalTimeZone() *time.Location {
	return time.FixedZone("CST", 8*3600) // UTC+8
}

func HandleLambdaEvent(event EvEnt) (string, error) {
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
	log.Printf("下载配置文件结束耗时:%d", time.Now().In(GetLocalTimeZone()).Unix()-now)
	h := md5.New()
	h.Write([]byte(config.Key))
	basePath := "/tmp/" + hex.EncodeToString(h.Sum(nil))
	err = os.MkdirAll(basePath, 0755)
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(basePath)
	tmp := strings.Split(config.Key, "/")
	out, _ := os.OpenFile(basePath+"/"+tmp[len(tmp)-1], os.O_CREATE|os.O_APPEND, 0755)
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
		out.Write(splitBody)
		split.Body.Close()
		log.Printf("下载分片文件:%s:读取大小:%d:写入大小:%d", s.Key, s.Size, len(splitBody))
	}
	out.Close()
	log.Printf("下载分片文件结束耗时:%d", time.Now().In(GetLocalTimeZone()).Unix()-now)
	now = time.Now().In(GetLocalTimeZone()).Unix()
	svc.PutObject(&s3.PutObjectInput{
		Body:   out,
		Bucket: aws.String(Bucket),
		Key:    aws.String(config.Key),
	})
	log.Printf("上传文件结束:耗时%d", time.Now().In(GetLocalTimeZone()).Unix()-now)
	return result.String(), err
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
