package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type EvEnt struct {
	Records []struct {
		AwsRegion         string `json:"awsRegion"`
		EventName         string `json:"eventName"`
		EventSource       string `json:"eventSource"`
		EventVersion      string `json:"eventVersion"`
		RequestParameters struct {
			SourceIPAddress string `json:"sourceIPAddress"`
		} `json:"requestParameters"`
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
	} `json:"records"`
}

type Config struct {
	Key   string  `json:"key"`
	Split []Split `json:"split"`
}
type Split struct {
	Key  string `json:"key"`
	Size int    `json:"size"`
}

func GetLocalTimeZone() *time.Location {
	return time.FixedZone("CST", 8*3600) // UTC+8
}

func HandleLambdaEvent(ctx context.Context, event EvEnt) (string, error) {
	log.Printf("start:%d", 1222)
	now := time.Now().In(GetLocalTimeZone()).Unix()
	now1 := now
	if len(event.Records) < 1 {
		return "", nil
	}
	Bucket := ""
	jsonFile := ""
	log.Printf("获得event")
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
	wg := sync.WaitGroup{}
	wg.Add(len(config.Split))
	log.Printf("开始下载分片文件")
	for _, s := range config.Split {
		go func(sp Split, sw *sync.WaitGroup) {
			log.Printf("下载分片%s文件开始", sp.Key)
			splitInput := &s3.GetObjectInput{
				Bucket: aws.String(Bucket),
				Key:    aws.String(sp.Key),
			}
			out, err := os.Create(basePath + "/" + sp.Key)
			if err != nil {
				log.Printf("错误1:%s", err.Error())
				sw.Done()
				return
			}
			suc := false
			for i := 0; i < 10; i++ {
				split, err := svc.GetObject(splitInput)
				if err == nil {
					_, err = io.Copy(out, split.Body)
					if err == nil {
						suc = true
						out.Close()
						break
					}
				}
				if i == 9 {
					if suc {
						log.Printf("下载分片%s文件结束", sp.Key)
					} else {
						log.Printf("错误:%s", err.Error())
					}
				}
			}
			sw.Done()
		}(s, &wg)
	}
	wg.Wait()
	log.Printf("下载分片文件结束")
	log.Printf("下载总耗时:%d", time.Now().In(GetLocalTimeZone()).Unix()-now)
	tmp := strings.Split(config.Key, "/")
	out, err := os.Create(basePath + "/" + tmp[len(tmp)-1])
	if err != nil {
		return "", err
	}
	writer := bufio.NewWriter(out)
	for _, s := range config.Split {
		fo, err := os.Open(basePath + "/" + s.Key)
		if err != nil {
			return "", err
		}
		f, err := ioutil.ReadAll(fo)
		if err != nil {
			return "", err
		}
		writer.Write(f)
		fo.Close()
		writer.Flush()
	}
	out.Close()
	now = time.Now().In(GetLocalTimeZone()).Unix()
	fileData, _ := os.Open(basePath + "/" + tmp[len(tmp)-1])
	svc.PutObject(&s3.PutObjectInput{
		Body:   fileData,
		Bucket: aws.String(Bucket),
		Key:    aws.String(config.Key),
	})
	fileData.Close()
	log.Printf("上传总耗时:%d", time.Now().In(GetLocalTimeZone()).Unix()-now)
	log.Printf("总耗时:%d", time.Now().In(GetLocalTimeZone()).Unix()-now1)
	return "", nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
