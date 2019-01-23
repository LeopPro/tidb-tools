package file_uploader

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"os"
	"path/filepath"
)

var _ = Suite(&testFileUploader{})

type testServerDriver struct{}

type MockFileUploaderDriver struct {
	targetDir string
}

func NewMockFileUploaderDriver(targetDir string) *MockFileUploaderDriver {
	return &MockFileUploaderDriver{targetDir}
}

func (m *MockFileUploaderDriver) Upload(sliceInfo *Slice) (string, error) {
	srcPath := filepath.Join(sliceInfo.FilePath, sliceInfo.FileName)
	srcFile, err := os.OpenFile(srcPath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer srcFile.Close()
	_, err = srcFile.Seek(sliceInfo.Offset, 0)
	if err != nil {
		return "", errors.Trace(err)
	}
	targetPath := filepath.Join(sliceInfo.FilePath, sliceInfo.FileName)
	targetFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer targetFile.Close()
	_, err = targetFile.Seek(sliceInfo.Offset, 0)
	if err != nil {
		return "", errors.Trace(err)
	}
	buf := make([]byte, 1024)
	hash := m.Hash()
	for {
		n, _ := srcFile.Read(buf)
		if 0 == n {
			break
		}
		_, err := hash.Write(buf[:n])
		if err != nil {
			return "", errors.Trace(err)
		}
		_, err = targetFile.Write(buf[:n])
		if err != nil {
			return "", errors.Trace(err)
		}
	}
	return hash.String(), nil
}

func (m *MockFileUploaderDriver) Hash() FileHash {
	return NewMd5Base64FileHash()
}
