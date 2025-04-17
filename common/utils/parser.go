package utils

import (
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func MustDropRow(field string) bool {
	return field == "" || field == "null" //  field == "[]" ||
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

func DictionaryToListName(input string) ([]string, error) {
	var result []string

	if input == "" { //  || input == "[]"
		return result, errors.New("input is empty")
	}

	if input == "[]" {
		return result, nil
	}
	var cleaned string

	if strings.Contains(input, "cast_id") {

		// 'key' => "key"
		reKey := regexp.MustCompile(`'([^']+)':`)
		cleaned = reKey.ReplaceAllString(input, `"$1":`)

		// 'value' => "value"
		reVal := regexp.MustCompile(`: '([^']*)'([,}])`)
		cleaned = reVal.ReplaceAllString(cleaned, `: "$1"$2`)
		reDoubleQuotesInValues := regexp.MustCompile(`:\s*"[^"]*"[^,}]*"[,}]`)

		// para: 'character': 'Roop Lal "Phillauri"',
		cleaned = reDoubleQuotesInValues.ReplaceAllStringFunc(cleaned, func(match string) string {

			value := match[3 : len(match)-2]
			if strings.Contains(value, `"`) {
				value = strings.ReplaceAll(value, `"`, `'`)
			}

			return `: "` + value + `"` + match[len(match)-1:]
		})
		cleaned = strings.ReplaceAll(cleaned, "None", "null")
	} else {
		cleaned = strings.ReplaceAll(input, "'", "\"")
	}
	var items []NameItem
	err := json.Unmarshal([]byte(cleaned), &items)
	if err != nil {
		var item NameItem
		err := json.Unmarshal([]byte(cleaned), &item)
		if err != nil {
			return result, err
		}
		return []string{item.Name}, nil
	}

	for _, item := range items {
		result = append(result, item.Name)
	}
	return result, nil
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
