package main

import (
	"bytes"
	"fmt"
	"lpp"
	"os"
	"strconv"
)

type ALNResult struct {
	Length    int
	Ref_list  [2]int
	Aln_list  [2]int
	Direct    string
	Reference string
	Perc      float64
}

func main() {
	FASTAIO := lpp.GetBlockRead( os.Args[1], "\n>", false, 10000000)
	contig_length := make(map[string]int)
	for {
		line, err := FASTAIO.Next()
		if err != nil {
			break
		}
		line = bytes.TrimSuffix(line, []byte(">"))
		line = bytes.TrimPrefix(line, []byte(">"))
		name := bytes.Fields(line)[0]
		seq := bytes.SplitN(line, []byte("\n"), 2)[1]
		seq = bytes.Replace(seq, []byte("\n"), []byte(""), -1)
		length := len(seq)
		contig_length[string(name)] = length

	}
	ALN_Result := make(map[string]*ALNResult)

	align_hash := make(map[string]map[string][][2]int)

	ref_hash := make(map[string]map[string][][2]int)
	RAWIO := lpp.GetBlockRead("", "\n", true, 100000)
	for {
		line, err := RAWIO.Next()
		if err != nil {
			break
		}
		line_l := bytes.Fields(bytes.TrimSuffix(line, []byte("\n")))
		ref_start, _ := strconv.Atoi(string(line_l[8]))
		ref_end, _ := strconv.Atoi(string(line_l[9]))
		aln_start, _ := strconv.Atoi(string(line_l[6]))
		aln_end, _ := strconv.Atoi(string(line_l[7]))
		//		ctg_l, _ := strconv.Atoi(string(line_l[8]))

		reference := string(line_l[1])
		query := string(line_l[0])

		if reference == query {
			continue
		}
		_, ok := ref_hash[reference]
		if !ok {
			ref_hash[reference] = make(map[string][][2]int)
		}
		_, ok = ref_hash[reference][query]
		if !ok {
			ref_hash[reference][query] = [][2]int{}
		}
		ref_hash[reference][query] = append(ref_hash[reference][query], [2]int{ref_start, ref_end})

		_, ok = align_hash[reference]
		if !ok {
			align_hash[reference] = make(map[string][][2]int)
		}
		_, ok = align_hash[reference][query]
		if !ok {
			align_hash[reference][query] = [][2]int{}
		}
		align_hash[reference][query] = append(align_hash[reference][query], [2]int{aln_start, aln_end})

	}
	//	fmt.Println(align_hash)
	//	fmt.Println(ref_hash)

	for each_ref, data_hash := range align_hash {
		for each_contig, _ := range data_hash {
			align1, ref1, length1 := lpp.COORD_CHAIN(align_hash[each_ref][each_contig], ref_hash[each_ref][each_contig], 0)
			align2, ref2, length2 := lpp.COORD_CHAIN(align_hash[each_ref][each_contig], ref_hash[each_ref][each_contig], 1)
			_, ok := ALN_Result[each_contig]
			if !ok {

				ALN_Result[each_contig] = &ALNResult{}
				if length2 > length1 {
					ALN_Result[each_contig].Perc = float64(length2) / float64(contig_length[each_contig])
					ALN_Result[each_contig].Length = length2
					ALN_Result[each_contig].Direct = "-"
					ALN_Result[each_contig].Aln_list = [2]int{align2[0][0], align2[len(align2)-1][1]}
					ALN_Result[each_contig].Reference = each_ref
					ALN_Result[each_contig].Ref_list = [2]int{ref2[0][0], ref2[len(ref2)-1][1]}
				} else {
					ALN_Result[each_contig].Length = length1
					ALN_Result[each_contig].Perc = float64(length1) / float64(contig_length[each_contig])
					ALN_Result[each_contig].Direct = "+"
					ALN_Result[each_contig].Aln_list = [2]int{align1[0][0], align1[len(align1)-1][1]}
					ALN_Result[each_contig].Reference = each_ref
					ALN_Result[each_contig].Ref_list = [2]int{ref1[0][0], ref1[len(ref1)-1][1]}
				}
			} else {
				if length2 > length1 {
					if length2 > ALN_Result[each_contig].Length {
						ALN_Result[each_contig].Perc = float64(length2) / float64(contig_length[each_contig])
						ALN_Result[each_contig].Length = length2
						ALN_Result[each_contig].Direct = "-"
						ALN_Result[each_contig].Aln_list = [2]int{align2[0][0], align2[len(align2)-1][1]}
						ALN_Result[each_contig].Reference = each_ref
						ALN_Result[each_contig].Ref_list = [2]int{ref2[0][0], ref2[len(ref2)-1][1]}
					}
				} else {
					if length1 > ALN_Result[each_contig].Length {
						ALN_Result[each_contig].Perc = float64(length1) / float64(contig_length[each_contig])
						ALN_Result[each_contig].Length = length1
						ALN_Result[each_contig].Direct = "+"
						ALN_Result[each_contig].Aln_list = [2]int{align1[0][0], align1[len(align1)-1][1]}
						ALN_Result[each_contig].Reference = each_ref
						ALN_Result[each_contig].Ref_list = [2]int{ref1[0][0], ref1[len(ref1)-1][1]}
					}

				}

			}

		}

	}
	//	align, _, length := lpp.COORD_CHAIN(align_hash["NC_036627.1"]["tig00000009"], ref_hash["NC_036627.1"]["tig00000009"], 1)
	//fmt.Println(align_hash["NC_036622.1"]["tig00000001"])
	FINAL_Result := make(map[string]map[int]string)
	for key, value := range ALN_Result {
		perc := value.Perc
		if perc > 0.7 || value.Length > 100000 {
			ref := value.Reference
			_, ok := FINAL_Result[ref]
			if !ok {
				FINAL_Result[ref] = make(map[int]string)
			}
			FINAL_Result[ref][value.Ref_list[0]] = key + value.Direct
		}

	}
	for key, value := range FINAL_Result {
		for ctg, coord := range value {
			fmt.Println(key, ctg, coord)
		}

	}
}
