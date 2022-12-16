package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"log"
	"net/http"
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
	log.Printf("start:%d", 12)
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
	ps := strings.Split(config.Split[0].Key, "/")
	fpath := basePath
	for i, p := range ps {
		if i == len(ps)-1 {
			break
		}
		fpath = fpath + "/" + p
	}
	err = os.MkdirAll(fpath, 0755)
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(basePath)
	wg := sync.WaitGroup{}
	log.Printf("开始下载分片文件")
	for i, s := range config.Split {
		if i == 0 || i%10 != 0 {
			wg.Add(1)
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
						}
					}
					if i == 9 || suc {
						if suc {
							log.Printf("下载分片%s文件结束", sp.Key)
							break
						} else {
							log.Printf("错误:%s", err.Error())
						}
					}
				}
				sw.Done()
			}(s, &wg)
		} else {
			log.Printf("下载分片%s文件开始", s.Key)
			splitInput := &s3.GetObjectInput{
				Bucket: aws.String(Bucket),
				Key:    aws.String(s.Key),
			}
			out, err := os.Create(basePath + "/" + s.Key)
			if err != nil {
				log.Printf("错误1:%s", err.Error())
				return "", err
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
				if i == 9 || suc {
					if suc {
						log.Printf("下载分片%s文件结束", s.Key)
					} else {
						log.Printf("错误:%s", err.Error())
					}
				}
			}
		}
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
	var chunkSize int64 = 26214400 //25M
	now = time.Now().In(GetLocalTimeZone()).Unix()
	name, size := getFileInfo(basePath + "/" + tmp[len(tmp)-1])
	PartsUpload(svc, Bucket, name, size, chunkSize)
	/*fileData, _ := os.Open(basePath + "/" + tmp[len(tmp)-1])
	svc.PutObject(&s3.PutObjectInput{
		Body:   fileData,
		Bucket: aws.String(Bucket),
		Key:    aws.String(config.Key),
	})
	fileData.Close()*/
	log.Printf("上传总耗时:%d", time.Now().In(GetLocalTimeZone()).Unix()-now)
	log.Printf("总耗时:%d", time.Now().In(GetLocalTimeZone()).Unix()-now1)
	return "", nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
}

//先获取文件名称和大小
func getFileInfo(filename string) (string, int64) {
	//文件操作
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	fileInfo, _ := file.Stat()

	return filename, fileInfo.Size()
}

func PartsUpload(svc *s3.S3, bucket, key string, size, chunkSize int64) {
	oneDay := 86400

	//文件操作
	file, err := os.Open(key)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	buffer := make([]byte, size)
	fileType := http.DetectContentType(buffer)
	if _, err := file.Read(buffer); err != nil {
		panic(err)
	}

	//给分片上传1天的过期时间
	expires := time.Now().Add(time.Duration(oneDay) * time.Second)
	//创建分片
	res, err := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:  &bucket,
		Key:     &key,
		Expires: &expires,
		//
		ContentType: aws.String(fileType),
	})
	if err != nil {
		panic(err)
	}
	var remainSize int64
	//获取分片个数
	chunkNum := size / chunkSize
	if size%chunkSize > 0 {
		chunkNum++
		remainSize = size % chunkSize
	}
	var completedParts []*s3.CompletedPart
	var i int64
	for ; i < chunkNum; i++ {
		partNum := i + 1
		partSize := chunkSize
		if partNum == chunkNum {
			if remainSize > 0 {
				partSize = remainSize
			}
		}
		fileBytes := buffer[chunkSize*i : chunkSize*i+partSize]
		//UploadPart=	req, out := c.UploadPartRequest(input)+req.Send()
		uploadResult, err := svc.UploadPart(&s3.UploadPartInput{
			Bucket:     &bucket,
			Key:        &key,
			PartNumber: aws.Int64(partNum),
			UploadId:   res.UploadId,
			//
			Body:          bytes.NewReader(fileBytes),
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})
		if err != nil {
			panic(err)
		}
		completedParts = append(completedParts, &s3.CompletedPart{
			ETag:       uploadResult.ETag,
			PartNumber: aws.Int64(partNum),
		})
	}

	compResp, err := svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: res.UploadId,
		//
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("compResp:%+v\n", compResp.String())
}
