package integration

import (
	"encoding/json"
	"fmt"
	"testing"
)

// Read Jest's machine report: successful process exit alone does not prove
// any assertions ran, and console wording is not a test result contract.
func validateSDKIntegrationReport(body []byte) (int, error) {
	var report struct {
		Success                   bool `json:"success"`
		NumTotalTests             int  `json:"numTotalTests"`
		NumPassedTests            int  `json:"numPassedTests"`
		NumFailedTests            int  `json:"numFailedTests"`
		NumPendingTests           int  `json:"numPendingTests"`
		NumTodoTests              int  `json:"numTodoTests"`
		NumRuntimeErrorTestSuites int  `json:"numRuntimeErrorTestSuites"`
		TestResults               []struct {
			Status           string `json:"status"`
			AssertionResults []struct {
				Status string `json:"status"`
			} `json:"assertionResults"`
		} `json:"testResults"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		return 0, fmt.Errorf("decode Jest report: %w", err)
	}
	if !report.Success || report.NumFailedTests != 0 || report.NumPendingTests != 0 || report.NumTodoTests != 0 || report.NumRuntimeErrorTestSuites != 0 {
		return 0, fmt.Errorf("Jest did not complete every test successfully")
	}
	passed := 0
	for _, suite := range report.TestResults {
		if suite.Status != "passed" {
			return 0, fmt.Errorf("SDK suite status is %q", suite.Status)
		}
		for _, assertion := range suite.AssertionResults {
			if assertion.Status != "passed" {
				return 0, fmt.Errorf("SDK test status is %q", assertion.Status)
			}
			passed++
		}
	}
	if passed == 0 || passed != report.NumTotalTests || passed != report.NumPassedTests {
		return 0, fmt.Errorf("Jest results do not reconcile: %d passed assertions, total=%d, passed=%d", passed, report.NumTotalTests, report.NumPassedTests)
	}
	return passed, nil
}

func TestSDKIntegrationReportReconcilesExecutedAssertions(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{"passed", `{"success":true,"numTotalTests":2,"numPassedTests":2,"testResults":[{"status":"passed","assertionResults":[{"status":"passed"},{"status":"passed"}]}]}`, 2},
		{"skipped case", `{"success":true,"numTotalTests":2,"numPassedTests":1,"numPendingTests":1,"testResults":[{"status":"passed","assertionResults":[{"status":"passed"},{"status":"pending"}]}]}`, 0},
		{"no tests", `{"success":true,"numTotalTests":0,"numPassedTests":0,"testResults":[]}`, 0},
		{"missing assertion results", `{"success":true,"numTotalTests":2,"numPassedTests":2,"testResults":[]}`, 0},
		{"suite setup failure", `{"success":false,"numRuntimeErrorTestSuites":1,"testResults":[{"status":"failed","assertionResults":[]}]}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			passed, err := validateSDKIntegrationReport([]byte(tc.body))
			if tc.want == 0 {
				if err == nil {
					t.Fatal("incomplete Jest execution was accepted")
				}
			} else if err != nil || passed != tc.want {
				t.Fatalf("passed=%d, err=%v; want %d", passed, err, tc.want)
			}
		})
	}
}
