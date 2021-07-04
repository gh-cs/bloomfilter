package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	mu         sync.Mutex
	itemNo     int
	fpProb     float64
	bitArray   []bool
	hashFuncNo int
}

type Item struct {
	Val string `json:"val"`
}

func CreateBloomFilter(itemNo int, fpProb float64) *BloomFilter {

	bitArraySize := -(float64(itemNo) * math.Log(fpProb)) / math.Pow(math.Log(2), 2)
	hashFuncNo := int((bitArraySize / float64(itemNo)) * math.Log(2))

	bitArray := make([]bool, int(bitArraySize))

	bf := &BloomFilter{
		mu:         sync.Mutex{},
		itemNo:     itemNo,
		fpProb:     fpProb,
		bitArray:   bitArray,
		hashFuncNo: hashFuncNo,
	}

	return bf
}

func (bf *BloomFilter) Check(w http.ResponseWriter, r *http.Request) {
	var x Item
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	err := json.NewDecoder(r.Body).Decode(&x)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.CheckItem(x.Val) {
		fmt.Fprintf(w, x.Val+" does not exist.\n")
	} else {
		fmt.Fprintf(w, x.Val+" exists.\n")
	}
	return
}

func (bf *BloomFilter) CheckItem(item string) bool {
	for i := 0; i < bf.hashFuncNo; i++ {
		digest := murmur3.Sum32WithSeed([]byte(item), uint32(i)) % uint32(len(bf.bitArray))
		if bf.bitArray[digest] == false { // if any bit corresponding to the digest is false, the item is not here
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Add(w http.ResponseWriter, r *http.Request) {
	var x Item
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	err := json.NewDecoder(r.Body).Decode(&x)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	bf.mu.Lock()
	defer bf.mu.Unlock()

	if !bf.CheckItem(x.Val) {
		digests := make([]uint32, bf.hashFuncNo)
		for i := 0; i < bf.hashFuncNo; i++ {
			digest := murmur3.Sum32WithSeed([]byte(x.Val), uint32(i)) % uint32(len(bf.bitArray))
			digests = append(digests, digest)
			bf.bitArray[digest] = true
			log.Println(digest)
		}
	} else {
		fmt.Fprintf(w, x.Val+" exists.\n")
	}
	return

}

func main() {
	r := mux.NewRouter()

	bf := CreateBloomFilter(10, 0.05)

	r.HandleFunc("/add", bf.Add).Methods("POST")
	r.HandleFunc("/check", bf.Check).Methods("POST")

	srv := &http.Server{
		Handler: r,
		Addr:    "localhost:8080",
	}

	log.Fatal(srv.ListenAndServe())
}
