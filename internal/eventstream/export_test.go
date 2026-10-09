package eventstream

// The feed head and Session signal statements, exported to the external test
// package so their query plans can be checked under the real Event Stream
// role.
const (
	SessionFeedHeadQueryForTest = sessionFeedHeadQuery
	ThreadFeedHeadQueryForTest  = threadFeedHeadQuery
	SessionSignalsQueryForTest  = sessionSignalsQuery
)
