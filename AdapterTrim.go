package main

import (
	"bufio"
	//	"bytes"
	"flag"
	"fmt"
	"lpp"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var adapter_hash map[string]string = make(map[string]string)

func Filter(f1 string, outputdir *string) {

	seq_hash := make(map[string]string)
	f2 := strings.Replace(f1, ".R1.", ".R2.", -1)

	FQ1IO := lpp.GetBlockRead(f1, "\n", false, 10000000)

	FQ2IO := lpp.GetBlockRead(f2, "\n", false, 10000000)
	resultname := strings.Split(f1, "_")[0]
	resultFile1, err1 := os.Create(*outputdir + "/" + resultname + ".R1.fastq")
	if err1 != nil {
		panic("Fastq not Exist " + *outputdir + resultname + ".R1.fastq")

	}
	OUTPUTBUF1 := bufio.NewWriterSize(resultFile1, 10000)
	resultFile12, err2 := os.Create(*outputdir + "/" + resultname + ".R2.fastq")
	if err2 != nil {
		panic("Fastq not Exist " + *outputdir + resultname + ".R2.fastq")

	}
	OUTPUTBUF2 := bufio.NewWriterSize(resultFile12, 10000)
	ResultIOLIST := [2]*bufio.Writer{OUTPUTBUF1, OUTPUTBUF2}
	FQ1Q2IO := PairFastq{}
	FQ1Q2IO.Pair1 = &FQ1IO
	FQ1Q2IO.Pair2 = &FQ2IO
	for {
		status := "NO"
		tag := &status
		fq1data, fq2data, err := FQ1Q2IO.Next()
		if err != nil {
			break
		}
		seq_f2 := string(fq2data[1])
		seq_all := string(fq1data[1]) + seq_f2
		max_score := float64(0)
		for adap, _ := range adapter_hash {
			score := lpp.SmithWaterman(adap, seq_f2)
			if score > max_score {
				max_score = score
			}

		}
		if max_score > 10 {
			*tag = "Yes"
		}
		if *tag == "Yes" {
			_, has := seq_hash[seq_all]
			if !has {

				for _, cont := range fq1data {
					ResultIOLIST[0].Write(cont)
				}
				for _, cont := range fq2data {
					ResultIOLIST[1].Write(cont)
				}
				seq_hash[seq_all] = ""
			}

		}

	}
	wg.Done()
}

type PairFastq struct {
	Pair1 *lpp.IO
	Pair2 *lpp.IO
}

var wg sync.WaitGroup

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

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() {
		err := recover()

		if err != nil {

			fmt.Println(err)

		}
	}()
	inputdir := flag.String("i", "", "Input")
	outputdir := flag.String("o", "", "Output")
	adapter := flag.String("a", "", "Adapter")

	flag.Parse()
	ADAPTER := lpp.Fasta{File: *adapter}
	for {
		_, seq, err := ADAPTER.Next()
		adapter_hash[string(seq)] = ""
		rev_seq := string(lpp.RevComplement(seq))
		adapter_hash[rev_seq] = ""
		if err != nil {
			break
		}

	}

	matches, _ := filepath.Glob(*inputdir + "/*R1.*fastq")

	wg.Add(len(matches))
	if _, err := os.Stat(*outputdir); os.IsNotExist(err) {
		os.Mkdir(*outputdir, os.ModePerm)
	}
	for _, e_f := range matches {
		go Filter(e_f, outputdir)

	}
	wg.Wait()

}
