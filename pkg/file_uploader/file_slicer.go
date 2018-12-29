package file_uploader

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
)

// FileSlicer slices file into `SliceInfo`.
// It can restore from checkpoint and designed for append only file.
// Don't worry about random write file, `Checker` will guarantee file consistency.
type FileSlicer struct {
	workDir     string
	sliceStatus *sliceStatus
}

type sliceStatus struct {
	statusFile     string
	SliceSize      int64            `json:"slice_size"`
	SliceTotalSize map[string]int64 `json:"slice_total_size"`
}

type SliceInfo struct {
	FileName string
	Index    int64
	Offset   int64
	Length   int64
}

const SliceStatusFile = ".fu_slice_status"

// NewFileSlicer creates a `FileSlicer` load from `statusFile`
func NewFileSlicer(workDir string, sliceSize int64) (*FileSlicer, error) {
	statusFile := filepath.Join(workDir, SliceStatusFile)
	sliceStatus, err := loadSliceStatus(statusFile, sliceSize)
	if err != nil {
		return nil, errors.Annotate(err, "error thrown during load slice status")
	}
	return &FileSlicer{
		workDir,
		sliceStatus,
	}, nil
}

func loadSliceStatus(statusFile string, sliceSize int64) (*sliceStatus, error) {
	var sliceStatus sliceStatus
	if _, err := os.Stat(statusFile); err == nil {
		// statusFile is exist
		jsonBytes, err := ioutil.ReadFile(statusFile)
		if err != nil {
			return nil, errors.Annotate(err, "error thrown during read statusFile file")
		}
		if err := json.Unmarshal(jsonBytes, &sliceStatus); err != nil {
			return nil, errors.Annotate(err, "error thrown during unmarshal json")
		}
		if sliceSize != sliceStatus.SliceSize {
			return nil, errors.New("can't restore from checkpoint, the slice_size is different from status file")
		}
	} else {
		sliceStatus.SliceTotalSize = make(map[string]int64)
		sliceStatus.SliceSize = sliceSize
	}
	sliceStatus.statusFile = statusFile
	return &sliceStatus, nil
}

func (ss *sliceStatus) flush() error {
	jsonBytes, err := json.Marshal(ss)
	if err != nil {
		return errors.Annotate(err, "error thrown during marshaling json")
	}
	statusFile, err := os.OpenFile(ss.statusFile, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return errors.Annotate(err, "error thrown during open statusFile file")
	}
	defer statusFile.Close()
	_, err = statusFile.Write(jsonBytes)
	if err != nil {
		return errors.Annotate(err, "error thrown during write statusFile file")
	}
	return nil
}

// DoSlice slices `file` and returns SliceInfo Arrays.
func (fs *FileSlicer) DoSlice(file os.FileInfo) ([]SliceInfo, error) {
	sliceSize := fs.sliceStatus.SliceSize
	fileName := file.Name()
	fileSize := file.Size()
	oldSliceTotalSize := fs.sliceStatus.SliceTotalSize[fileName]
	fs.sliceStatus.SliceTotalSize[fileName] = fileSize
	var sliceInfos []SliceInfo
	var i int64
	for i = oldSliceTotalSize / sliceSize; i*sliceSize < fileSize; i++ {
		thisSliceSize := sliceSize
		if thisSliceSize+i*sliceSize > fileSize {
			thisSliceSize = fileSize - i*sliceSize
		}
		sliceInfo := SliceInfo{
			FileName: fileName,
			Index:    i,
			Offset:   i * sliceSize,
			Length:   thisSliceSize,
		}
		sliceInfos = append(sliceInfos, sliceInfo)
	}
	err := fs.sliceStatus.flush()
	if err != nil {
		return nil, errors.Annotate(err, "error thrown during flushing slice status")
	}
	return sliceInfos, nil
}
