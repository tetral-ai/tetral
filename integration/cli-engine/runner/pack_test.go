package main

import (
	"fmt"
	"slices"
	"testing"
)

// TestPack holds the packing rule: a table's rows stay in table order and
// fill a job up to the limit, a row at or over the limit is a job of its
// own, and the longest jobs come first.
func TestPack(t *testing.T) {
	rowsOf := func(test string, budgets ...int) []row {
		rows := make([]row, len(budgets))
		for i, budget := range budgets {
			rows[i] = row{name: fmt.Sprintf("%s/r%d", test, i), budget: budget, index: i}
		}
		return rows
	}
	for _, c := range []struct {
		name string
		rows []row
		want []string
	}{
		{"rows fill a job up to the limit", rowsOf("TestA", 25, 35, 70, 70, 25, 40), []string{
			"j00-TestA-1 200 ^TestA$/^(r0|r1|r2|r3)$",
			"j01-TestA-2 65 ^TestA$/^(r4|r5)$",
		}},
		{"a row at or over the limit is a job of its own", rowsOf("TestB", 100, 100, 60, 200, 140, 330), []string{
			"j00-TestB-5 330 ^TestB$/^(r5)$",
			"j01-TestB-1 200 ^TestB$/^(r0|r1)$",
			"j02-TestB-3 200 ^TestB$/^(r3)$",
			"j03-TestB-4 140 ^TestB$/^(r4)$",
			"j04-TestB-2 60 ^TestB$/^(r2)$",
		}},
		{"table order is the listed index", []row{{"TestC/b", 30, 1}, {"TestC/a", 20, 0}}, []string{
			"j00-TestC-1 50 ^TestC$/^(a|b)$",
		}},
		{"a test that is no table row runs alone", []row{{"TestD", 105, 0}, {"TestE/x", 25, 0}}, []string{
			"j00-TestD-1 105 ^TestD$",
			"j01-TestE-1 25 ^TestE$/^(x)$",
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got []string
			for _, j := range pack(c.rows, 200, "j") {
				got = append(got, fmt.Sprintf("%s %d %s", j.name, j.budget, j.pattern()))
			}
			if !slices.Equal(got, c.want) {
				t.Errorf("pack gave\n%q\nwant\n%q", got, c.want)
			}
		})
	}
}
