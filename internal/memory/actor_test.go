package memory

import "testing"

func TestActorDiscriminatedUnion(t *testing.T) {
	for _, actor := range []Actor{
		{Type: ActorAPI, APIKeyID: "ak_actor"},
		{Type: ActorUser, UserID: "identity_human"},
		{Type: ActorService, ServiceID: "identity_service"},
		{Type: ActorSession, SessionID: "sesn_actor"},
	} {
		if err := validateActor(actor); err != nil {
			t.Fatalf("valid actor %s rejected: %v", actor.Type, err)
		}
	}
	for _, actor := range []Actor{
		{Type: ActorService},
		{Type: ActorService, UserID: "identity_human", ServiceID: "identity_service"},
		{Type: ActorService, APIKeyID: "ak_actor", ServiceID: "identity_service"},
		{Type: ActorService, SessionID: "sesn_actor", ServiceID: "identity_service"},
		{Type: ActorAPI, APIKeyID: "ak_actor", ServiceID: "identity_service"},
		{Type: ActorUser, UserID: "identity_human", ServiceID: "identity_service"},
		{Type: ActorSession, SessionID: "sesn_actor", ServiceID: "identity_service"},
	} {
		if err := validateActor(actor); err == nil {
			t.Fatalf("ambiguous actor accepted: %+v", actor)
		}
	}
}
