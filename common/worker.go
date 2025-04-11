package common

type Row struct {
	Numerics map[string]uint     `json:"numerics"`
	Strings  map[string]string   `json:"strings"`
	Arrays   map[string][]string `json:"arrays"`
	Booleans map[string]bool     `json:"booleans"`
}
