package top_map_reduce

import (
	"reflect"
	"testing"
)

// TestMergeSort runs a comprehensive test suite for the MergeSort function
func TestMergeSort(t *testing.T) {
	// Helper function to compare slices
	assertEqual := func(t *testing.T, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	// Test cases
	tests := []struct {
		name      string
		list1     []int
		list2     []int
		isGreater func(a, b int) bool
		topSize   uint
		expected  []int
	}{
		// Basic Functionality
		{"Basic Test 1", []int{5, 3, 1}, []int{6, 4, 2}, func(a, b int) bool { return a > b }, 6, []int{6, 5, 4, 3, 2, 1}},
		{"Basic Test 2", []int{5, 3, 1}, []int{5, 4, 2}, func(a, b int) bool { return a > b }, 6, []int{5, 5, 4, 3, 2, 1}},
		{"Basic Test 3", []int{5, 5, 5}, []int{5, 5, 5}, func(a, b int) bool { return a > b }, 6, []int{5, 5, 5, 5, 5, 5}},

		// Edge Cases
		{"One Empty List", []int{}, []int{6, 4, 2}, func(a, b int) bool { return a > b }, 3, []int{6, 4, 2}},
		{"Both Empty Lists", []int{}, []int{}, func(a, b int) bool { return a > b }, 3, []int{}},
		{"TopSize Zero", []int{5, 3, 1}, []int{6, 4, 2}, func(a, b int) bool { return a > b }, 0, []int{}},
		{"TopSize Greater Than Total", []int{5, 3, 1}, []int{6, 4, 2}, func(a, b int) bool { return a > b }, 10, []int{6, 5, 4, 3, 2, 1}},

		// Truncation
		{"Truncation Test 1", []int{5, 3, 1}, []int{6, 4, 2}, func(a, b int) bool { return a > b }, 4, []int{6, 5, 4, 3}},
		{"Truncation Test 2", []int{5, 3, 1}, []int{6, 4, 2}, func(a, b int) bool { return a > b }, 6, []int{6, 5, 4, 3, 2, 1}},

		// Custom Comparator
		{"Custom Comparator Ascending", []int{1, 3, 5}, []int{2, 4, 6}, func(a, b int) bool { return a < b }, 6, []int{1, 2, 3, 4, 5, 6}},
	}

	// Run each test case
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeSort(tt.list1, tt.list2, tt.isGreater, tt.topSize)
			assertEqual(t, got, tt.expected)
		})
	}
}
