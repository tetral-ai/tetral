// Package ciflow holds the three decisions a pull-request workflow makes
// outside its tests: which products' jobs a change runs, whether a gate
// passes on the results of the jobs it needs, and whether the run took too
// long. It imports the standard library only, so the commands that call it
// build in seconds on a fresh runner.
package ciflow
