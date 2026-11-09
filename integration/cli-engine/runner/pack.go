package main

import (
	"bufio"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// row is one listed row: a table's row, TestX/name, or a test that is no
// table row, TestX.
type row struct {
	name string
	// budget is the row's time budget in seconds on the lane's OS.
	budget int
	// index is the row's place in its table.
	index int
}

func (r row) test() string {
	test, _, _ := strings.Cut(r.name, "/")
	return test
}

func (r row) subtest() string {
	_, subtest, _ := strings.Cut(r.name, "/")
	return subtest
}

// listing is what the harness listed for one lane.
type listing struct {
	rows []row
	// persons are the tests that need a person at the Mac, never run here.
	persons []string
}

// parseListing reads the harness's list mode output (TETRAL_JOINT_LIST=1).
func parseListing(output string) (listing, error) {
	var l listing
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		switch fields[0] {
		case "joint-row":
			if len(fields) != 4 {
				return l, fmt.Errorf("listing line %q has %d fields; want 4", scanner.Text(), len(fields))
			}
			budget, err := strconv.Atoi(fields[2])
			if err != nil || budget <= 0 {
				return l, fmt.Errorf("listing line %q has no positive budget", scanner.Text())
			}
			index, err := strconv.Atoi(fields[3])
			if err != nil {
				return l, fmt.Errorf("listing line %q has no table index", scanner.Text())
			}
			l.rows = append(l.rows, row{name: fields[1], budget: budget, index: index})
		case "joint-person":
			if len(fields) == 2 {
				l.persons = append(l.persons, fields[1])
			}
		}
	}
	return l, scanner.Err()
}

// job is the rows that one test process runs one after another on one
// Engine, as a serial run of their table would.
type job struct {
	name string
	rows []row
	// budget is the sum of the rows' budgets.
	budget int
}

// pattern is the job's -test.run pattern.
func (j job) pattern() string {
	test := "^" + regexp.QuoteMeta(j.rows[0].test()) + "$"
	if j.rows[0].subtest() == "" {
		return test
	}
	names := make([]string, len(j.rows))
	for i, r := range j.rows {
		names[i] = regexp.QuoteMeta(r.subtest())
	}
	return test + "/^(" + strings.Join(names, "|") + ")$"
}

// pack packs each table's rows, in table order, into jobs of at most limit
// budget-seconds; a row of limit or more is a job of its own. The jobs come
// longest first, each named prefix, its place in that order, its test and
// its number within the test.
func pack(rows []row, limit int, prefix string) []job {
	var tests []string
	byTest := map[string][]row{}
	for _, r := range rows {
		if _, ok := byTest[r.test()]; !ok {
			tests = append(tests, r.test())
		}
		byTest[r.test()] = append(byTest[r.test()], r)
	}
	var jobs []job
	for _, test := range tests {
		table := byTest[test]
		slices.SortStableFunc(table, func(a, b row) int { return a.index - b.index })
		var current job
		n := 0
		flush := func() {
			if len(current.rows) > 0 {
				n++
				current.name = fmt.Sprintf("%s-%d", test, n)
				jobs = append(jobs, current)
				current = job{}
			}
		}
		for _, r := range table {
			if r.budget >= limit || current.budget+r.budget > limit {
				flush()
			}
			current.rows = append(current.rows, r)
			current.budget += r.budget
			if r.budget >= limit {
				flush()
			}
		}
		flush()
	}
	slices.SortStableFunc(jobs, func(a, b job) int { return b.budget - a.budget })
	for i := range jobs {
		jobs[i].name = fmt.Sprintf("%s%02d-%s", prefix, i, jobs[i].name)
	}
	return jobs
}
