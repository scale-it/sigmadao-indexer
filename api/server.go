package api

import "time"

// ExtraOptions are options which change the behavior or the HTTP server.
type ExtraOptions struct {
	// Tokens are the access tokens which can access the API.
	Tokens []string

	// DeveloperMode turns on features like AddressSearchRoundRewind
	DeveloperMode bool

	// MetricsEndpoint turns on the /metrics endpoint for prometheus metrics.
	MetricsEndpoint bool

	// MetricsEndpointVerbose generates separate histograms based on query parameters on the /metrics endpoint.
	MetricsEndpointVerbose bool

	// Maximum amount of time to wait before timing out writes to a response. Note that handler timeout is computed
	//off of this.
	WriteTimeout time.Duration

	// ReadTimeout is the maximum duration for reading the entire request, including the body.
	ReadTimeout time.Duration

	// DisabledMapConfig is the disabled map configuration that is being used by the server
	DisabledMapConfig *DisabledMapConfig

	// MaxAPIResourcesPerAccount is the maximum number of combined AppParams, AppLocalState, AssetParams,
	// and AssetHolding resources per address that can be returned by the /v2/accounts endpoints.
	// If an address exceeds this number, a 400 error is returned. Zero means unlimited.
	MaxAPIResourcesPerAccount uint64

	/////////////////////
	// Limit Constants //
	/////////////////////

	// Transactions
	MaxTransactionsLimit     uint64
	DefaultTransactionsLimit uint64

	// Accounts
	MaxAccountsLimit     uint64
	DefaultAccountsLimit uint64

	// Assets
	MaxAssetsLimit     uint64
	DefaultAssetsLimit uint64

	// Asset Balances
	MaxBalancesLimit     uint64
	DefaultBalancesLimit uint64

	// Applications
	MaxApplicationsLimit     uint64
	DefaultApplicationsLimit uint64
}
