package tetralcleanup

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/tetral-ai/tetral/internal/workload"
)

const openMetricsContentType = "application/openmetrics-text; version=1.0.0; charset=utf-8"

type MetricsExporter interface {
	Export(context.Context, []workload.Metric) error
}

type OpenMetricsHTTPExporter struct {
	Endpoint string
	Client   *http.Client
}

func (e OpenMetricsHTTPExporter) Export(ctx context.Context, metrics []workload.Metric) error {
	if e.Endpoint == "" {
		return nil
	}
	body := workload.RuntimeMetricsTextWith(metrics) + "# EOF\n"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.Endpoint, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", openMetricsContentType)
	client := e.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("metrics exporter returned HTTP %d", response.StatusCode)
	}
	return nil
}
