package pilosa

import "testing"

func TestCallCallback(t *testing.T) {
	req := QueryRequest{
		DB:          "2",
		CallbackURL: "http://localhost:8080/blah",
	}
	bicliques := []Biclique{{Tiles: []uint64{1, 2, 3}, Count: 54, Score: 87}}
	resultBatch := make([]interface{}, 0)
	for _, bc := range bicliques {
		resultBatch = append(resultBatch, bc)
	}

	callCallback(resultBatch, &req)
}
