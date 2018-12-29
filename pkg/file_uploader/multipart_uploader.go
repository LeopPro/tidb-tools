package file_uploader

import (
	"encoding/json"
	"github.com/juju/errors"
	"github.com/siddontang/go/sync2"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type MultipartUploader struct {
	workDir string
}

func (mu *MultipartUploader) upload(si *SliceInfo) error {
	filePath := filepath.Join(mu.workDir, si.FileName)
	fileInfo, err := os.Stat(filePath)
	if err != nil || fileInfo.IsDir() || fileInfo.Size() < si.Offset+si.Length {
		log.Warnf("file is't exist or slice offset over the file size, FileInfo:%#v SliceInfo %#v", fileInfo, si)
	}
	// check log
	//TODO update with driver and handle error
	// add status log
	panic("12")
}

var checkPointRunning sync2.AtomicInt32

const CheckPointFile = ".fu_check_point"

type checkPoint struct {
	status         map[string]map[int64]indexCheckPoint
	rwLock         *sync.RWMutex
	checkPointFile string
}

type indexCheckPoint struct {
	Uploaded bool      `json:"uploaded"`
	Date     time.Time `json:"date"`
	Hash     string    `json:"hash"`
	Offset   int64     `json:"offset"`
	Length   int64     `json:"length"`
}

func loadCheckPoint(workDir string) (*checkPoint, error) {
	if !checkPointRunning.CompareAndSwap(0, 1) {
		return nil, errors.New("checkPoint is already running")
	}
	checkPoint := checkPoint{rwLock: new(sync.RWMutex)}
	checkPoint.checkPointFile = filepath.Join(workDir, CheckPointFile)
	if _, err := os.Stat(checkPoint.checkPointFile); err == nil {
		// checkPointFile is exist
		jsonBytes, err := ioutil.ReadFile(checkPoint.checkPointFile)
		if err != nil {
			return nil, errors.Annotate(err, "error thrown during read checkPointFile file")
		}
		if err := json.Unmarshal(jsonBytes, &checkPoint.status); err != nil {
			return nil, errors.Annotate(err, "error thrown during unmarshal json")
		}
	} else {
		checkPoint.status = make(map[string]map[int64]indexCheckPoint)
	}
	return &checkPoint, nil
}

func (cp *checkPoint) logSliceUpload(si *SliceInfo, hash string, successful bool) error {
	cp.rwLock.Lock()
	defer cp.rwLock.Unlock()
	fileCp, exist := cp.status[si.FileName]
	if !exist {
		fileCp = make(map[int64]indexCheckPoint)
		cp.status[si.FileName] = fileCp
	}
	fileCp[si.Index] = indexCheckPoint{successful, time.Now(), hash, si.Offset, si.Length}

	jsonBytes, err := json.Marshal(cp.status)
	if err != nil {
		return errors.Annotate(err, "error thrown during marshaling json")
	}
	checkPointFile, err := os.OpenFile(cp.checkPointFile, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return errors.Annotate(err, "error thrown during open checkPointFile file")
	}
	defer checkPointFile.Close()
	_, err = checkPointFile.Write(jsonBytes)
	if err != nil {
		return errors.Annotate(err, "error thrown during write checkPointFile file")
	}
	return nil
}

func (cp *checkPoint) isSliceUploadSuccessful(si *SliceInfo) bool {
	cp.rwLock.RLock()
	defer cp.rwLock.RUnlock()
	fileCp, exist := cp.status[si.FileName]
	if !exist {
		return exist
	}
	indexCp, exist := fileCp[si.Index]
	if !exist {
		return exist
	}
	return indexCp.Uploaded && indexCp.Offset == si.Offset && indexCp.Length == si.Length
}

func (cp *checkPoint) checkHash(si *SliceInfo, hash string) bool {
	cp.rwLock.RLock()
	defer cp.rwLock.RUnlock()
	indexCp := cp.status[si.FileName][si.Index]
	return hash == indexCp.Hash && indexCp.Uploaded &&
		indexCp.Offset == si.Offset && indexCp.Length == si.Length
}
