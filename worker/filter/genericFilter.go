package filter

import "github.com/ptourne/sistemas-distribuidos-1/common"

type NumericOperator int

const (
	Equal NumericOperator = iota
	NotEqual
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
)

type NumericCondition struct {
	Column   string
	Value    uint
	Operator NumericOperator
}

// func (c NumericCondition) Passes(row common.Row) (bool, error) {
// 	if val, ok := row.Arrays["production_countries"]; ok {
// 		switch c.Operator {
// 		case Equal:
// 			return row.Numerics[c.Column] == c.Value
// 		case NotEqual:
// 			return row.Numerics[c.Column] != c.Value
// 		case GreaterThan:
// 			return row.Numerics[c.Column] > c.Value
// 		case LessThan:
// 			return row.Numerics[c.Column] < c.Value
// 		case GreaterThanOrEqual:
// 			return row.Numerics[c.Column] >= c.Value
// 		case LessThanOrEqual:
// 			return row.Numerics[c.Column] <= c.Value
// 		default:
// 			return false
// 		}
// 	}
// }

type Condition interface {
	Passes(row common.Row) bool
}

type GenericFilter struct {
	Conditions []Condition
}

// func (f GenericFilter) Process(row common.Row) *common.Row {
// 	if val, ok := row.Arrays["production_countries"]; ok {
// 		if !(len(val) == 1) {
// 			return nil
// 		}
// 	} else {
// 		return nil
// 	}
// 	return &common.Row{
// 		Strings: map[string]string{
// 			"movieID": row.Strings["movieID"],
// 			"title":   row.Strings["title"],
// 			"country": row.Arrays["production_countries"][0],
// 		},
// 		Numerics: map[string]uint{
// 			"budget": row.Numerics["budget"],
// 		},
// 	}
// }
