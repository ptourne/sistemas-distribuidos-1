package utils

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

func MustDropRow(field string) bool {
	return field == "" || field == "[]" || field == "null" // || field == "0" || field == "0.0"
}

func ExtractYear(dateStr string) (uint, bool) { // TODO: Handle empty strings
	if dateStr == "" {
		return 0, false
	}
	date, err := time.Parse(time.DateOnly, dateStr) // Formato Go: siempre 2006-01-02
	if err != nil {
		return 0, false
	}

	return uint(date.Year()), true
}

type NameItem struct {
	Name string `json:"name"`
}

type NameCountry struct {
	ISO string `json:"iso_3166_1"`
}

func DictionaryToListName(input string) ([]string, bool) {
	var result []string

	if input == "" || input == "[]" {
		return result, false
	}

	cleaned := strings.ReplaceAll(input, "'", "\"")
	var items []NameItem
	err := json.Unmarshal([]byte(cleaned), &items)
	if err != nil {
		var item NameItem
		err := json.Unmarshal([]byte(cleaned), &item)
		if err != nil {
			return result, false
		}
		return []string{item.Name}, true
	}

	for _, item := range items {
		result = append(result, item.Name)
	}
	return result, true
}

func DictionaryToListIso(input string) ([]string, bool) {
	var result []string

	if input == "" || input == "[]" {
		return result, false
	}

	cleaned := strings.ReplaceAll(input, "'", "\"")
	var items []NameCountry
	err := json.Unmarshal([]byte(cleaned), &items)
	if err != nil {
		var item NameCountry
		err := json.Unmarshal([]byte(cleaned), &item)
		if err != nil {
			return result, false
		}
		return []string{item.ISO}, true
	}

	for _, item := range items {
		result = append(result, item.ISO)
	}
	return result, true
}

func ParseFloat(s string) (float64, bool) { // TODO: Handle empty strings
	if s == "" {
		return 0.0, false
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0, false
	}
	return value, true
}
