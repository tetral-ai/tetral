package jobrunner

func completionMailEnvelope(taskName string, sender string, payload string) string {
	return "Message Type: FINAL_ANSWER\nTask name: " + taskName + "\nSender: " + sender + "\nPayload:\n" + payload
}
