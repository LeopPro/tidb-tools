package utils

//
//import "bytes"
//
//type BlockBuffer struct {
//	bu          bytes.Buffer
//	maxSize     int
//	currentSize int
//}
//
//func NewBlockBuffer(maxSize int) *BlockBuffer {
//	return &BlockBuffer{maxSize: maxSize}
//}
//
//func (bb *BlockBuffer) Write(p []byte) (n int, err error) {
//	for bb.currentSize+len(p) > bb.maxSize {
//
//	}
//	n, err = bb.bu.Write(p)
//	bb.currentSize += n
//}
//
//func (bb *BlockBuffer) Read(p []byte) (n int, err error) {
//	n, err = bb.bu.Read(p)
//	bb.currentSize -= n
//}
