package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"math"
	"strconv"
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
	log.Printf("start:%d", 3)
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
		log.Printf("获取数据失败")
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
	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(config.Key),
	}
	cmuRes, err := svc.CreateMultipartUpload(createMultipartUploadInput)
	if err != nil {
		log.Printf("发生错误%s:", err.Error())
		return "", err
	}
	syncMap := sync.Map{}
	wg := sync.WaitGroup{}

	maxSize := len(config.Split)
	size := 10
	step := int(math.Ceil(float64(maxSize) / float64(size)))
	for i := 0; i < step; i++ {
		min := i * size
		max := (i + 1) * size
		if max >= len(config.Split) {
			max = len(config.Split)
		}
		tmpS := config.Split[min:max:max]
		for j, s := range tmpS {
			go func(sp Split, sw *sync.WaitGroup, svc1 *s3.S3, start, index int) {
				log.Printf("下载分片%s文件开始", sp.Key)
				splitInput := &s3.GetObjectInput{
					Bucket: aws.String(Bucket),
					Key:    aws.String(sp.Key),
				}
				split, err := svc.GetObject(splitInput)
				if err != nil {
					log.Printf("下载分片错误1:%s:%s", sp.Key, err.Error())
					return
				}
				upInput := &s3.UploadPartInput{
					Body:       aws.ReadSeekCloser(split.Body),
					Bucket:     aws.String(Bucket),
					Key:        aws.String(config.Key),
					PartNumber: aws.Int64(int64(start+index) + 1),
					UploadId:   cmuRes.UploadId,
				}
				upResult, err := svc.UploadPart(upInput)
				if err != nil {
					log.Printf("下载分片错误2:%s:%s", sp.Key, err.Error())
					return
				}
				syncMap.Store(strconv.Itoa(start+index+1), upResult.ETag)
				split.Body.Close()
				log.Printf("下载分片%s文件成功", sp.Key)
				sw.Done()
			}(s, &wg, svc, min, j)
		}
		wg.Wait()
	}

	datas := make([]*s3.CompletedPart, 0)
	syncMap.Range(func(key, value any) bool {
		partNumber, _ := strconv.ParseInt(key.(string), 10, 64)
		if partNumber < 1 {
			log.Printf("合并错误3:%s:%s", key, value)
			return false
		}
		data := &s3.CompletedPart{
			ETag:       aws.String(value.(string)),
			PartNumber: aws.Int64(partNumber),
		}
		datas = append(datas, data)
		return true
	})
	if len(datas) != len(config.Split) {
		log.Printf("合并错误4:%s", "上传分片数不一致")
		return "", nil
	}
	log.Printf("提交合并结束请求")
	cinput := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(config.Key),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: datas,
		},
		UploadId: cmuRes.UploadId,
	}
	res, err := svc.CompleteMultipartUpload(cinput)
	if err != nil {
		log.Printf("错误5:%s", err.Error())
		return "", nil
	}
	log.Printf("提交合并结束请求")
	log.Printf("success:%s", res.String())
	log.Printf("总耗时:%d", time.Now().In(GetLocalTimeZone()).Unix()-now1)
	return "", nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
}
