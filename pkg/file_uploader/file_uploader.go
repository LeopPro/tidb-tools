package file_uploader

import (
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/watcher"
	"os"
	"path/filepath"
	"time"
)

var slicesChan chan Slice

/*
1 watcher to slice
2 slice to multi uploader
3 mock driver
4 test
*/
type FileUploader struct {
	workDir      string
	watcher      *watcher.Watcher
	slicer       *FileSlicer
	fileUploader FileUploaderDriver
}

func NewFileUploader(workDir string, slicesSize int64, fileUploader FileUploaderDriver) *FileUploader {
	watcher := watcher.NewWatcher()
	// TODO close watcher
	err := watcher.Add(workDir)
	if err != nil {
		log.Errorf("watcher load failure: %#v", err)
	}
	err = watcher.Start(5 * time.Second)
	if err != nil {
		log.Errorf("watcher load failure: %#v", err)
	}
	fileSlicer, err := NewFileSlicer(workDir, slicesSize)
	fu := &FileUploader{workDir, watcher, fileSlicer, fileUploader}
	fu.createWorker(5)
	go fu.process()
	return fu
}
func (fu *FileUploader) createWorker(workerNum int) {
	cp, err := loadCheckPoint(fu.workDir)
	if err != nil {
		log.Fatalf("check point load failure: %#v", err)
		os.Exit(-1)
	}
	for i := 0; i < workerNum; i++ {
		go func() {
			mu := NewMultipartUploader(fu.workDir, cp, fu.fileUploader)
			for {
				select {
				case slice := <-slicesChan:
					err := mu.upload(&slice)
					log.Errorf("slice %#v upload failure: %#v", slice, err)
				}
				// handler close
			}
		}()
	}
}

func (fu *FileUploader) process() {
	workFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Trace(err)
		}
		if info.IsDir() {
			return nil
		}
		slices, err := fu.slicer.DoSlice(info)
		for _, slice := range slices {
			slicesChan <- slice
		}
		return nil
	}
	err := filepath.Walk(fu.workDir, workFunc)
	log.Errorf("watch workDir failure: %#v", err)
	for {
		select {
		case ev := <-fu.watcher.Events:
			err := workFunc(ev.Path, ev.FileInfo, nil)
			log.Errorf("watch workDir failure: %#v", err)
		case err2 := <-fu.watcher.Errors:
			log.Errorf("watch workDir failure: %#v", err2)
		}
		// handler close
	}

}
