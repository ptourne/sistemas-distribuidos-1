package common

import (
	"testing"

	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
)

func TestDictionaryToListName(t *testing.T) {
	input := "[{'name': 'John \"wick\"', 'age': 30, 'city': 'New York'}, {'name': 'John wick2', 'age': 30, 'city': 'New York'}, {'name': \"Tom' cruise\", 'age': 30, 'city': 'New York'}]"
	expected := []string{"John \"wick\"", "John wick2", "Tom' cruise"}
	res, err := utils.DictionaryToListName(input)

	if err != nil {
		t.Errorf("Error: %v", err)
	}

	for i, v := range res {
		if v != expected[i] {
			t.Errorf("Expected %v, but got %v", expected[i], v)
		}
	}
}
