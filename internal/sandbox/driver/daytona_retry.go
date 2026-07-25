package driver

import (
	"context"
	"errors"
	"net/http"
	"time"

	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
)

const daytonaTransientMaxAttempts = 3

func retryDaytonaTransient[T any](
	ctx context.Context,
	call func() (T, error),
	wait func(context.Context, time.Duration) error,
) (T, error) {
	var zero T
	if call == nil {
		return zero, errors.New("daytona retry call is required")
	}
	if wait == nil {
		wait = waitForDaytonaRetry
	}
	delay := 100 * time.Millisecond
	for attempt := 1; attempt <= daytonaTransientMaxAttempts; attempt++ {
		value, err := call()
		if err == nil || !isDaytonaTransientError(err) || attempt == daytonaTransientMaxAttempts {
			return value, err
		}
		if err := wait(ctx, delay); err != nil {
			return zero, err
		}
		delay *= 2
	}
	return zero, errors.New("daytona retry attempts exhausted")
}

func retryDaytonaTransientError(ctx context.Context, call func() error) error {
	_, err := retryDaytonaTransient(ctx, func() (struct{}, error) {
		return struct{}{}, call()
	}, nil)
	return err
}

func isDaytonaTransientError(err error) bool {
	var rateLimited *daytonaerrors.DaytonaRateLimitError
	if errors.As(err, &rateLimited) {
		return true
	}
	var server *daytonaerrors.DaytonaServerError
	if errors.As(err, &server) {
		return server.StatusCode >= http.StatusInternalServerError
	}
	var daytonaErr *daytonaerrors.DaytonaError
	return errors.As(err, &daytonaErr) &&
		(daytonaErr.StatusCode == http.StatusTooManyRequests || daytonaErr.StatusCode >= http.StatusInternalServerError)
}

func waitForDaytonaRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
