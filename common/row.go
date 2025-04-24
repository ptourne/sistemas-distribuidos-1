package common

type Row struct {
	Numerics map[string]uint     `json:"n"`
	Strings  map[string]string   `json:"s"`
	Arrays   map[string][]string `json:"a"`
	Floats   map[string]float64  `json:"f"`
	Type 	 TypeRow             `json:"t"`
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