// Package ciflow holds the two decisions a pull-request workflow makes
// outside its tests, which products' jobs a change runs and whether a gate
// passes on the results of the jobs it needs, and the report of how long a
// finished run's jobs waited and ran. It imports the standard library only,
// so the commands that call it build in seconds on a fresh runner.
package ciflow
