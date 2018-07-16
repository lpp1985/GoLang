package lpp

import (
	"bufio"
	"bytes"

	"io"

	"os"
)

type Block_Reading struct {
	File     *os.File
	Blocktag string
	Buffer   int
}
type IO struct {
	Io       *bufio.Reader
	BlockTag []byte
	SplitTag byte
}

/*
使用说明
FQ2HANDLE, errfq2 := os.Open(*fastq1)
if errfq2 != nil {
	panic(*fastq2 + " Is not Exist!!")
}
FQ2IO := lpp.GetBlockRead(FQ2HANDLE, "\n", false, 10000000)


*/
func (blockreading *Block_Reading) Read() IO {
	BlockIO := IO{}

	raw_file := blockreading.File
	if blockreading.Buffer == 0 {
		blockreading.Buffer = 99999999
	}
	BlockIO.Io = bufio.NewReaderSize(raw_file, blockreading.Buffer)
	if blockreading.Blocktag == "" {
		BlockIO.BlockTag = []byte("\n")
	} else {
		BlockIO.BlockTag = []byte(blockreading.Blocktag)
	}
	BlockIO.SplitTag = byte([]byte(blockreading.Blocktag)[len(blockreading.Blocktag)-1])

	return BlockIO

}
func GetBlockRead(filehandle string, blocktag string, header bool, buffer int) IO {
	BR := new(Block_Reading)
	BR.Blocktag = blocktag
	BR.Buffer = buffer
	FILE, errfile := os.Open(filehandle)
	if errfile != nil {
		panic(filehandle + " Is not Exist!!")
	}
	BR.File = FILE
	Result_IO := BR.Read()

	if header {
		Result_IO.Next()
	}
	return Result_IO
}
func (Reader IO) Next() ([]byte, error) {

	var out_tag []byte
	var status error

	for {
		line, err := Reader.Io.ReadSlice(Reader.SplitTag)
		status = err
		out_tag = append(out_tag, line...)
		if err == nil {

			if len(Reader.BlockTag) > 1 {

				if len(out_tag) >= len(Reader.BlockTag) && bytes.Equal(out_tag[(len(out_tag)-len(Reader.BlockTag)):], Reader.BlockTag) {

					break
				}

			} else {
				break
			}

		} else if err == io.EOF {
			break
		} else if err != bufio.ErrBufferFull {
			break
		}

	}

	return out_tag, status

}
