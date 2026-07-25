package gitproxy

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

type gitEndpoint string

const (
	endpointRefsUpload  gitEndpoint = "refs-upload"
	endpointRefsReceive gitEndpoint = "refs-receive"
	endpointUploadPack  gitEndpoint = "upload-pack"
	endpointReceivePack gitEndpoint = "receive-pack"
)

var gitSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

type gitRequest struct {
	LegacyTicket string
	Owner        string
	Repo         string
	RepoWithGit  string
	GitPath      string
	Endpoint     gitEndpoint
	UpstreamPath string
}

func parseGitRequest(request *http.Request, legacyPathCutover bool) (gitRequest, bool) {
	if request == nil || request.URL == nil {
		return gitRequest{}, false
	}
	rawPath := request.URL.Path
	if rawPath == "" || strings.Contains(rawPath, "//") {
		return gitRequest{}, false
	}
	segments := strings.Split(strings.TrimPrefix(rawPath, "/"), "/")
	legacyTicket := ""
	if len(segments) > 0 && segments[0] != "github.com" {
		if !legacyPathCutover {
			return gitRequest{}, false
		}
		legacyTicket = segments[0]
		segments = segments[1:]
	}
	if len(segments) < 4 || segments[0] != "github.com" {
		return gitRequest{}, false
	}
	owner := segments[1]
	repoWithGit := segments[2]
	repo := strings.TrimSuffix(repoWithGit, ".git")
	if !validGitSegment(owner) || !validGitSegment(repo) || repo == "" {
		return gitRequest{}, false
	}
	base := gitRequest{
		LegacyTicket: legacyTicket,
		Owner:        owner,
		Repo:         repo,
		RepoWithGit:  repoWithGit,
	}
	switch request.Method {
	case http.MethodGet:
		if len(segments) != 5 || segments[3] != "info" || segments[4] != "refs" {
			return gitRequest{}, false
		}
		query, err := url.ParseQuery(request.URL.RawQuery)
		if err != nil {
			return gitRequest{}, false
		}
		services := query["service"]
		if len(query) != 1 || len(services) != 1 {
			return gitRequest{}, false
		}
		switch services[0] {
		case "git-upload-pack":
			base.GitPath = "/info/refs"
			base.Endpoint = endpointRefsUpload
		case "git-receive-pack":
			base.GitPath = "/info/refs"
			base.Endpoint = endpointRefsReceive
		default:
			return gitRequest{}, false
		}
	case http.MethodPost:
		if len(segments) != 4 || request.URL.RawQuery != "" {
			return gitRequest{}, false
		}
		switch segments[3] {
		case "git-upload-pack":
			base.GitPath = "/git-upload-pack"
			base.Endpoint = endpointUploadPack
		case "git-receive-pack":
			base.GitPath = "/git-receive-pack"
			base.Endpoint = endpointReceivePack
		default:
			return gitRequest{}, false
		}
	default:
		return gitRequest{}, false
	}
	base.UpstreamPath = "/" + owner + "/" + repoWithGit + base.GitPath
	return base, true
}

func validGitSegment(value string) bool {
	return gitSegmentPattern.MatchString(value)
}
