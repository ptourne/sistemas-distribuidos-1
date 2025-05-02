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
	cleaned := input

	// 'key' => "key"
	reKey := regexp.MustCompile(`'([^']+)':`)
	cleaned = reKey.ReplaceAllString(input, `"$1":`)
	// values
	reVal := regexp.MustCompile(`"\s*:\s*'(.+?)'[,}]`)
	cleaned = reVal.ReplaceAllStringFunc(cleaned, func(match string) string {
		start := strings.Index(match, "'") + 1
		end := strings.LastIndex(match, "'")
		if start <= 0 || end <= start {
			return match
		}
		value := match[start:end]
		value = strings.ReplaceAll(value, `\'`, `'`)

		var escaped strings.Builder
		for i := 0; i < len(value); i++ {
			if value[i] == '"' {
				if i == 0 || value[i-1] != '\\' {

					escaped.WriteByte('\\')
				}
			}
			escaped.WriteByte(value[i])
		}

		suffix := match[len(match)-1:]
		return `" : "` + escaped.String() + `"` + suffix
	})

	reVal2 := regexp.MustCompile(`: ''`)
	cleaned = reVal2.ReplaceAllString(cleaned, `: ""`)
	cleaned = strings.ReplaceAll(cleaned, "\\xa0", " ")
	cleaned = strings.ReplaceAll(cleaned, "\\xad", "-")
	cleaned = strings.ReplaceAll(cleaned, "\\x92", "")

	cleaned = strings.ReplaceAll(cleaned, "None", "null")

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

	if input == "" { // || input == "[]" {
		return result, false
	}

	if input == "[]" {
		return result, true
	}

	// 'key' => "key"
	reKey := regexp.MustCompile(`'([^']+)':`)
	cleaned := reKey.ReplaceAllString(input, `"$1":`)
	// values
	reVal := regexp.MustCompile(`"\s*:\s*'(.+?)'[,}]`)
	cleaned = reVal.ReplaceAllStringFunc(cleaned, func(match string) string {
		start := strings.Index(match, "'") + 1
		end := strings.LastIndex(match, "'")
		if start <= 0 || end <= start {
			return match
		}
		value := match[start:end]
		value = strings.ReplaceAll(value, `\'`, `'`)

		var escaped strings.Builder
		for i := 0; i < len(value); i++ {
			if value[i] == '"' {
				if i == 0 || value[i-1] != '\\' {

					escaped.WriteByte('\\')
				}
			}
			escaped.WriteByte(value[i])
		}

		suffix := match[len(match)-1:]
		return `" : "` + escaped.String() + `"` + suffix
	})

	reVal2 := regexp.MustCompile(`: ''`)
	cleaned = reVal2.ReplaceAllString(cleaned, `: ""`)
	cleaned = strings.ReplaceAll(cleaned, "\\xa0", " ")
	cleaned = strings.ReplaceAll(cleaned, "\\xad", "-")
	cleaned = strings.ReplaceAll(cleaned, "\\x92", "")

	cleaned = strings.ReplaceAll(cleaned, "None", "null")
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

func ParseUint64(s string) (uint64, bool) { // TODO: Handle empty strings
	if s == "" {
		return 0, false
	}
	value, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
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
