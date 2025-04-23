package common

type Row struct {
	Numerics map[string]uint     `json:"numerics"`
	Strings  map[string]string   `json:"strings"`
	Arrays   map[string][]string `json:"arrays"`
	Floats   map[string]float64  `json:"floats"`
	Type 	 TypeRow             `json:"type"`
}

type TypeRow int

const (
	QueryName TypeRow = iota
	QueryRow
	FinishQuerys
)

func RowQueryName(query string) *Row {
	return &Row{Strings: map[string]string{"type": query}, Type: QueryName}
}

func RowQuery(row Row) *Row {
	row.Type = QueryRow
	return &row
}