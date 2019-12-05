package main

import (
	"bufio"
	//	"bytes"
	"bytes"
	"flag"
	"fmt"
	"lpp"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

func Write_Pair(name string, fq1data [4][]byte, fq2data [4][]byte) {
	Check_File(name)
	for _, cont := range fq1data {

		output_has[name][0].Write(cont)
	}
	for _, cont := range fq2data {

		output_has[name][1].Write(cont)
	}
}
func Check_File(name string) {
	if _, o_has := output_has[name]; !o_has {
		resultFile1, err1 := os.Create(*outputdir + "/" + name + ".R1.fastq")
		if err1 != nil {
			panic("Fastq not Exist " + *outputdir + name + ".R1.fastq")

		}
		OUTPUTBUF1 := bufio.NewWriterSize(resultFile1, 10000)
		resultFile12, err2 := os.Create(*outputdir + "/" + name + ".R2.fastq")
		if err2 != nil {
			panic("Fastq not Exist " + *outputdir + name + ".R2.fastq")

		}
		OUTPUTBUF2 := bufio.NewWriterSize(resultFile12, 10000)

		output_has[name] = [2]*bufio.Writer{OUTPUTBUF1, OUTPUTBUF2}
	}
}
func Filter(f1 string, outputdir *string) {

	f2 := strings.Replace(f1, "_1.fq", "_2.fq", -1)

	FQ1IO := lpp.GetBlockRead(f1, "\n", false, 10000000)

	FQ2IO := lpp.GetBlockRead(f2, "\n", false, 10000000)

	FQ1Q2IO := PairFastq{}
	FQ1Q2IO.Pair1 = &FQ1IO
	FQ1Q2IO.Pair2 = &FQ2IO
	for {
		rev_data, for_data := "", ""
		has := true
		fq1data, fq2data, err := FQ1Q2IO.Next()
		if err != nil {
			break
		}
		seq_f1 := string(fq1data[1])[:8]

		if for_data, has = forward_has[seq_f1]; !has {
			Write_Pair("Error", fq1data, fq2data)
			continue
		}

		seq_f2 := string(fq2data[1])[:8]
		if rev_data, has = reverse_has[seq_f2]; !has {
			Write_Pair("Error", fq1data, fq2data)
			continue
		}
		resultname := "F_" + for_data + "___R_" + rev_data

		Write_Pair(resultname, fq1data, fq2data)

	}
	wg.Done()
}

type PairFastq struct {
	Pair1 *lpp.IO
	Pair2 *lpp.IO
}

var wg sync.WaitGroup
var output_has = make(map[string][2]*bufio.Writer)

func (Self *PairFastq) Next() ([4][]byte, [4][]byte, error) {
	data1 := [4][]byte{}
	data2 := [4][]byte{}
	i := 0
	for {
		i += 1
		line1, err1 := Self.Pair1.Next()
		if err1 != nil {
			return data1, data2, err1
		}
		data1[i-1] = line1
		line2, _ := Self.Pair2.Next()
		data2[i-1] = line2
		if i == 4 {
			return data1, data2, err1
		}

	}

}

var forward_has = make(map[string]string)
var reverse_has = make(map[string]string)
var outputdir *string

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() {
		err := recover()

		if err != nil {

			fmt.Println(err)

		}
	}()
	inputdir := flag.String("i", "", "Input")
	outputdir = flag.String("o", "", "Output")
	Forward := flag.String("f", "", "Forward")
	Reverse := flag.String("r", "", "Reverse")

	flag.Parse()
	seqIO := lpp.Fasta{File: *Forward}
	nospace, _ := regexp.Compile(`\s+`)
	for {
		title, sequence, err := seqIO.Next()
		name := bytes.Fields(title)[0][1:]

		forward_has[nospace.ReplaceAllString(string(sequence), "")] = string(name)

		if err != nil {
			break
		}
	}
	seqIO = lpp.Fasta{File: *Reverse}
	for {
		title, sequence, err := seqIO.Next()
		name := bytes.Fields(title)[0][1:]

		reverse_has[nospace.ReplaceAllString(string(sequence), "")] = string(name)

		if err != nil {
			break
		}
	}

	matches, _ := filepath.Glob(*inputdir + "/*_1.fq")

	wg.Add(len(matches))
	if _, err := os.Stat(*outputdir); os.IsNotExist(err) {
		os.Mkdir(*outputdir, os.ModePerm)
	}
	for _, e_f := range matches {
		go Filter(e_f, outputdir)

	}
	wg.Wait()

	for _, data := range output_has {
		data[0].Flush()
		data[1].Flush()
	}

}
