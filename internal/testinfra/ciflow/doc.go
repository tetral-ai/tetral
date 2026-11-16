// Package ciflow holds the two decisions a pull-request workflow makes
// outside its tests, which products' jobs a change runs and whether a gate
// passes on the results of the jobs it needs, the report of how long a
// finished run's jobs waited and ran, and the issue a scheduled run on main
// opens when it is red and closes when it is green again. It imports the
// standard library only, so the commands that call it build in seconds on a
// fresh runner.
package ciflow
