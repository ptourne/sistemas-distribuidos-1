package filter

func NewFilterReleaseDateL2010AndIncludeES() GenericFilter {
	return GenericFilter{
		Conditions:        []Condition{NumericCondition{"release_date", LessThan, 2010}, ArrayIncludes{"production_countries", "ES"}},
		KeptStringFields:  []string{"movieID", "title"},
		KeptNumericFields: []string{},
		KeptFloatFields:   []string{},
		KeptArrayFields:   []string{"genres"},
		Maps:              []Map{MapProductionCountriesa{}},
	}
}
