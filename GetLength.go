package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"lpp"
	"os"
)

func main() {
	err := recover()
	if err != nil {
		fmt.Println(err)
	}
	fasta := flag.String("f", "", "fasta")

	output := flag.String("o", "", "Output")

	flag.Parse()

	OUTPUTHANDLE, err := os.Create(*output)
	defer OUTPUTHANDLE.Close()
	OUTPUTBUF := bufio.NewWriterSize(OUTPUTHANDLE, 100000)
	OUTPUTBUF.WriteString("Name\tLength\n")
	defer OUTPUTBUF.Flush()
	if err != nil {
		panic("Output not Exist!!")
	}
	FASTAHANDLE, err := os.Open(*fasta)
	defer FASTAHANDLE.Close()
	if err != nil {
		panic("Fasta not Exist")
	}
	FASTAIO := lpp.GetBlockRead(FASTAHANDLE, "\n>", false, 10000000)
	for {
		line, err := FASTAIO.Next()

		line = bytes.TrimSuffix(line, []byte(">"))
		line = bytes.TrimPrefix(line, []byte(">"))
		name := bytes.Fields(line)[0]
		seq := bytes.SplitN(line, []byte("\n"), 2)[1]
		seq = bytes.Replace(seq, []byte("\n"), []byte(""), -1)
		length := len(seq)
		OUTPUTBUF.WriteString(fmt.Sprintf("%s\t%d\n", name, length))
		if err != nil {
			break
		}
	}

}
