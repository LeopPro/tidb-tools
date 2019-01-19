package file_uploader

import . "github.com/pingcap/check"

var _ = Suite(&testFileUploader{})

type testFileUploader struct{}

func (t *testFileUploader) TestFileUploaderAppend(c *C) {

	// open file uploader
	// write files
	// close file uploader
	// check file
}
