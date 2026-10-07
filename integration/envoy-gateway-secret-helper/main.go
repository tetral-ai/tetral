// This test-only module imports the pinned upstream internal translator.
// It repairs egctl's SecretType omission without replacing policy resources.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"sort"

	eg "github.com/envoyproxy/gateway/api/v1alpha1"
	"github.com/envoyproxy/gateway/internal/gatewayapi"
	"github.com/envoyproxy/gateway/internal/gatewayapi/resource"
	"github.com/envoyproxy/gateway/internal/infrastructure/common"
	"github.com/envoyproxy/gateway/internal/infrastructure/kubernetes/ratelimit"
	"github.com/envoyproxy/gateway/internal/ir"
	"github.com/envoyproxy/gateway/internal/logging"
	"github.com/envoyproxy/gateway/internal/xds/bootstrap"
	"github.com/envoyproxy/gateway/internal/xds/translator"
	adminv3 "github.com/envoyproxy/go-control-plane/envoy/admin/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

var stage = "input"

func main() {
	if run() != nil {
		_, _ = io.WriteString(os.Stderr, "pinned upstream SecretType extraction or resource parity failed at "+stage+"\n")
		os.Exit(1)
	}
}

func run() error {
	var input struct {
		Output struct {
			IR  map[string]json.RawMessage `json:"xdsIR"`
			XDS map[string]json.RawMessage `json:"xds"`
		} `json:"output"`
		FixtureYAML   []byte `json:"fixtureYAML"`
		FixtureSHA256 string `json:"fixtureSHA256"`
	}
	if err := json.NewDecoder(io.LimitReader(os.Stdin, 64<<20)).Decode(&input); err != nil {
		return err
	}
	if len(input.Output.IR) != 1 || len(input.Output.XDS) != 1 {
		return errors.New("require exactly one Gateway")
	}
	stage = "fixture_input_binding"
	digest := sha256.Sum256(input.FixtureYAML)
	if len(input.FixtureYAML) == 0 || input.FixtureSHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("fixture input identity differs")
	}
	resources, err := resource.LoadResourcesFromYAMLBytes(input.FixtureYAML, false, nil)
	if err != nil || resources.GatewayClass == nil {
		return errors.New("original explicit fixture resources are invalid")
	}
	for _, service := range resources.Services {
		if net.ParseIP(service.Spec.ClusterIP) == nil {
			return errors.New("fixture Service lacks an explicit endpoint IP")
		}
	}
	stage = "upstream_gateway_translation"
	gt := &gatewayapi.Translator{
		GatewayControllerName:  string(resources.GatewayClass.Spec.ControllerName),
		GatewayClassName:       gwapiv1.ObjectName(resources.GatewayClass.Name),
		GlobalRateLimitEnabled: true, EndpointRoutingDisabled: true,
		EnvoyPatchPolicyEnabled: true, BackendEnabled: true,
		Logger: logging.DefaultLogger(io.Discard, eg.LogLevelInfo),
	}
	translated, err := gt.Translate(context.Background(), resources)
	if translated == nil || len(translated.XdsIR) != 1 {
		return errors.New("original fixture Gateway translation differs")
	}
	unusedControlPlaneTLS := false
	if err != nil {
		// The pinned CLI discards all Gateway translator errors. Limit that
		// behavior here to its unconditional, unused controller Secret lookup;
		// unexpected errors and any real consumer still fail closed.
		if !unusedControlPlaneTLSFailure(err, resources, translated.XdsIR) {
			return errors.New("original fixture Gateway translation failed")
		}
		unusedControlPlaneTLS = true
	}
	for name, ir := range translated.XdsIR {
		original, found := input.Output.XDS[name]
		if !found {
			return errors.New("Gateway IR identity differs")
		}
		stage = "original_ir_parity"
		// JSON intentionally redacts PrivateBytes. Compare the entire same upstream
		// IR representation, then retain real private material from original YAML.
		freshIR, err := json.Marshal(ir)
		if err != nil {
			return err
		}
		var fresh, emitted any
		decodeIR := func(body []byte, into *any) error {
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.UseNumber()
			return decoder.Decode(into)
		}
		if decodeIR(freshIR, &fresh) != nil || decodeIR(input.Output.IR[name], &emitted) != nil || !reflect.DeepEqual(fresh, emitted) {
			return errors.New("original CLI IR differs from original fixture translation")
		}
		stage = "upstream_translation"
		tr := &translator.Translator{GlobalRateLimit: &translator.GlobalRateLimitSettings{ServiceURL: ratelimit.GetServiceURL("envoy-gateway-system", "cluster.local")}, Logger: logging.DefaultLogger(io.Discard, eg.LogLevelInfo)}
		if resources.EnvoyProxyForGatewayClass != nil {
			tr.FilterOrder = resources.EnvoyProxyForGatewayClass.Spec.FilterOrder
		}
		out, err := tr.Translate(context.Background(), ir)
		if err != nil {
			return err
		}
		stage = "original_dump_json"
		dump := &adminv3.ConfigDump{}
		var rawDump struct {
			Configs []json.RawMessage `json:"configs"`
		}
		if err := json.Unmarshal(original, &rawDump); err != nil {
			return err
		}
		policyConfigs := []json.RawMessage{}
		for _, rawConfig := range rawDump.Configs {
			var kind struct {
				Type string `json:"@type"`
			}
			if err := json.Unmarshal(rawConfig, &kind); err != nil {
				return err
			}
			// Bootstrap remains untouched in the original egctl snapshot. It carries
			// process-only extension types outside the policy translator's registry.
			if kind.Type == "type.googleapis.com/envoy.admin.v3.BootstrapConfigDump" {
				continue
			}
			stage = "original_dump_proto"
			config := &anypb.Any{}
			if err := protojson.Unmarshal(rawConfig, config); err != nil {
				return err
			}
			dump.Configs = append(dump.Configs, config)
			policyConfigs = append(policyConfigs, rawConfig)
		}
		stage = "original_resource_proto"
		common := map[string]map[string]proto.Message{}
		for _, config := range dump.Configs {
			message, err := anypb.UnmarshalNew(config, proto.UnmarshalOptions{})
			if err != nil {
				return err
			}
			add := func(typeURL string, resource *anypb.Any) error {
				value, err := anypb.UnmarshalNew(resource, proto.UnmarshalOptions{})
				if err != nil {
					return err
				}
				key := cachev3.GetResourceName(value)
				if common[typeURL] == nil {
					common[typeURL] = map[string]proto.Message{}
				}
				if _, duplicate := common[typeURL][key]; duplicate {
					return errors.New("duplicate original resource")
				}
				common[typeURL][key] = value
				return nil
			}
			switch config := message.(type) {
			case *adminv3.BootstrapConfigDump:
			case *adminv3.EndpointsConfigDump:
				for _, x := range config.DynamicEndpointConfigs {
					if err := add(resourcev3.EndpointType, x.EndpointConfig); err != nil {
						return err
					}
				}
			case *adminv3.ClustersConfigDump:
				for _, x := range config.DynamicActiveClusters {
					if err := add(resourcev3.ClusterType, x.Cluster); err != nil {
						return err
					}
				}
			case *adminv3.ListenersConfigDump:
				for _, x := range config.DynamicListeners {
					if err := add(resourcev3.ListenerType, x.GetActiveState().GetListener()); err != nil {
						return err
					}
				}
			case *adminv3.RoutesConfigDump:
				for _, x := range config.DynamicRouteConfigs {
					if err := add(resourcev3.RouteType, x.RouteConfig); err != nil {
						return err
					}
				}
			default:
				return errors.New("unexpected original dump type")
			}
		}
		stage = "common_resource_parity"
		counts := map[string]int{}
		secrets := map[string]*tlsv3.Secret{}
		for typeURL, resources := range out.XdsResources {
			if typeURL == resourcev3.SecretType {
				for _, resource := range resources {
					secret, ok := resource.(*tlsv3.Secret)
					if !ok {
						return errors.New("unexpected secret type")
					}
					if prior := secrets[secret.Name]; prior != nil && !proto.Equal(prior, secret) {
						return errors.New("conflicting duplicate secret")
					}
					secrets[secret.Name] = secret
				}
				continue
			}
			expected, found := common[typeURL]
			if !found || len(resources) != len(expected) {
				return errors.New("common resource set differs")
			}
			for _, resource := range resources {
				key := cachev3.GetResourceName(resource)
				if !proto.Equal(resource, expected[key]) {
					return errors.New("common resource proto differs")
				}
				delete(expected, key)
			}
			if len(expected) != 0 {
				return errors.New("common resource omitted")
			}
			counts[typeURL] = len(resources)
			delete(common, typeURL)
		}
		if len(common) != 0 {
			return errors.New("original resource type omitted")
		}
		stage = "secret_reference_parity"
		var raw any
		policyJSON, err := json.Marshal(policyConfigs)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(policyJSON, &raw); err != nil {
			return err
		}
		references := map[string]bool{}
		var walk func(any)
		walk = func(value any) {
			switch value := value.(type) {
			case map[string]any:
				for key, child := range value {
					if key == "tlsCertificateSdsSecretConfigs" {
						for _, entry := range child.([]any) {
							references[entry.(map[string]any)["name"].(string)] = true
						}
					}
					if key == "validationContextSdsSecretConfig" {
						references[child.(map[string]any)["name"].(string)] = true
					}
					walk(child)
				}
			case []any:
				for _, child := range value {
					walk(child)
				}
			}
		}
		walk(raw)
		if len(references) != len(secrets) {
			return errors.New("SecretType reference set differs")
		}
		names := []string{}
		for name := range secrets {
			if !references[name] {
				return errors.New("unreferenced secret")
			}
			names = append(names, name)
		}
		sort.Strings(names)
		stage = "secret_serialization"
		serialized := []json.RawMessage{}
		for _, name := range names {
			stage = "secret_tls_material"
			if certificate := secrets[name].GetTlsCertificate(); certificate != nil {
				if _, err := tls.X509KeyPair(certificate.GetCertificateChain().GetInlineBytes(), certificate.GetPrivateKey().GetInlineBytes()); err != nil {
					return errors.New("upstream fixture Secret lacks usable matching TLS material")
				}
			}
			if validation := secrets[name].GetValidationContext(); validation != nil {
				if !x509.NewCertPool().AppendCertsFromPEM(validation.GetTrustedCa().GetInlineBytes()) {
					return errors.New("upstream fixture Secret lacks usable trust material")
				}
			}
			stage = "secret_serialization"
			body, err := protojson.Marshal(secrets[name])
			if err != nil {
				return err
			}
			serialized = append(serialized, body)
		}
		stage = "upstream_proxy_process_args"
		if len(translated.InfraIR) != 1 || translated.InfraIR[name] == nil || translated.InfraIR[name].Proxy == nil {
			return errors.New("original proxy infrastructure identity differs")
		}
		drainArgs, err := upstreamProxyDrainArgs(translated.InfraIR[name].Proxy)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Gateway               string            `json:"gateway"`
			Common                map[string]int    `json:"commonResourceCounts"`
			Secrets               []json.RawMessage `json:"secrets"`
			FixtureSHA256         string            `json:"fixtureSHA256"`
			UnusedControlPlaneTLS bool              `json:"unusedControlPlaneTLS"`
			ProxyDrainArgs        []string          `json:"proxyDrainArgs"`
		}{name, counts, serialized, input.FixtureSHA256, unusedControlPlaneTLS, drainArgs})
	}
	return errors.New("missing Gateway")
}

// ProcessGlobalResources in Gateway v1.9.2 looks up this Secret before checking
// its consumers. These predicates match globalresources.go containsGlobalRateLimit
// and containsWasm exactly. ControllerNamespace is empty in the actual egctl
// options above; no controller Secret or synthetic default is supplied here.
func unusedControlPlaneTLSFailure(err error, resources *resource.Resources, xdsIRs resource.XdsIRMap) bool {
	if err == nil || err.Error() != "envoy TLS secret /envoy not found" || len(resources.ExtensionServerPolicies) != 0 || len(xdsIRs) != 1 {
		return false
	}
	for _, xdsIR := range xdsIRs {
		if xdsIR == nil {
			return false
		}
		for _, listener := range xdsIR.HTTP {
			for _, route := range listener.Routes {
				if route.Traffic != nil && route.Traffic.RateLimit != nil && route.Traffic.RateLimit.Global != nil {
					return false
				}
				if route.EnvoyExtensions != nil && len(route.EnvoyExtensions.Wasms) > 0 {
					return false
				}
			}
		}
	}
	return true
}

// The Kubernetes provider passes this exact translated proxy configuration to
// BuildProxyArgs (resource.go:125/resource_provider.go:496). Extract only the
// process drain options; original egctl Bootstrap and policy remain served.
func upstreamProxyDrainArgs(infra *ir.ProxyInfra) ([]string, error) {
	if infra == nil || infra.Config == nil {
		return nil, errors.New("missing translated proxy configuration")
	}
	args, err := common.BuildProxyArgs(infra, infra.Config.Spec.Shutdown, &bootstrap.RenderBootstrapConfigOptions{}, "tetral-local-official-snapshot", false)
	if err != nil {
		return nil, err
	}
	result := []string{}
	seen := map[string]bool{}
	for i := 0; i < len(args); i++ {
		if args[i] != "--drain-strategy" && args[i] != "--drain-time-s" {
			continue
		}
		if seen[args[i]] || i+1 >= len(args) {
			return nil, errors.New("invalid upstream drain option")
		}
		seen[args[i]] = true
		result = append(result, args[i], args[i+1])
		i++
	}
	if len(result) != 4 {
		return nil, errors.New("upstream drain options omitted")
	}
	return result, nil
}
