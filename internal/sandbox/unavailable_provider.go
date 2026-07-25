package sandbox

import "context"

type unavailableProvider struct{}

func NewUnavailableLifecycleProvider() LifecycleProvider {
	return unavailableProvider{}
}

func (unavailableProvider) CreateSandbox(context.Context, CreateSandboxRequest) (ProviderHandle, error) {
	return ProviderHandle{}, providerUnconfiguredError()
}

func (unavailableProvider) StartSandbox(context.Context, ProviderHandle) error {
	return providerUnconfiguredError()
}

func (unavailableProvider) CheckBaseTemplateHealth(context.Context, ProviderHandle) error {
	return providerUnconfiguredError()
}

func (unavailableProvider) ApplyNetworkPolicy(context.Context, ProviderHandle, NetworkSetup) error {
	return providerUnconfiguredError()
}

func (unavailableProvider) PrepareBaseDirectories(context.Context, ProviderHandle) error {
	return providerUnconfiguredError()
}

func (unavailableProvider) GetStatus(context.Context, ProviderHandle) (ProviderStatus, error) {
	return ProviderStatus{}, providerUnconfiguredError()
}

func (unavailableProvider) ReleaseSandbox(context.Context, ProviderHandle, ReleaseReason) error {
	return providerUnconfiguredError()
}

func providerUnconfiguredError() error {
	return &SandboxError{Code: SandboxErrorProviderUnconfigured, Message: "sandbox provider is not configured"}
}
