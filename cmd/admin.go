package cmd

import (
	"errors"
	"fmt"
	"fs/utils"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	defaultAdminEndpoint = "http://localhost:3000"
	defaultAdminRegion   = "us-east-1"
)

type adminOptions struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	JSON      bool
	Timeout   time.Duration
}

func newAdminCommand(build BuildInfo) *cobra.Command {
	opts := &adminOptions{}

	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Admin operations over the fs admin API",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.PersistentFlags().StringVar(&opts.Endpoint, "endpoint", "", "Admin API endpoint (env: FSCLI_ENDPOINT, fallback ADDRESS+PORT)")
	cmd.PersistentFlags().StringVar(&opts.Region, "region", "", "SigV4 region (env: FSCLI_REGION or FS_AUTH_REGION)")
	cmd.PersistentFlags().StringVar(&opts.AccessKey, "access-key", "", "Admin access key (env: FS_ROOT_USER, FSCLI_ACCESS_KEY)")
	cmd.PersistentFlags().StringVar(&opts.SecretKey, "secret-key", "", "Admin secret key (env: FS_ROOT_PASSWORD, FSCLI_SECRET_KEY)")
	cmd.PersistentFlags().BoolVar(&opts.JSON, "json", false, "Emit JSON output")
	cmd.PersistentFlags().DurationVar(&opts.Timeout, "timeout", 15*time.Second, "HTTP timeout")

	cmd.AddCommand(newAdminUserCommand(opts))
	cmd.AddCommand(newAdminDiagCommand(opts, build))
	return cmd
}

func (o *adminOptions) resolve(requireCredentials bool) error {
	serverCfg := utils.NewConfig()
	o.Endpoint = strings.TrimSpace(firstNonEmpty(
		o.Endpoint,
		os.Getenv("FSCLI_ENDPOINT"),
		endpointFromServerConfig(serverCfg.Address, serverCfg.Port),
		defaultAdminEndpoint,
	))
	o.Region = strings.TrimSpace(firstNonEmpty(
		o.Region,
		os.Getenv("FSCLI_REGION"),
		os.Getenv("FS_AUTH_REGION"),
		serverCfg.AuthRegion,
		defaultAdminRegion,
	))
	o.AccessKey = strings.TrimSpace(firstNonEmpty(
		o.AccessKey,
		os.Getenv("FS_ROOT_USER"),
		os.Getenv("FSCLI_ACCESS_KEY"),
		os.Getenv("AWS_ACCESS_KEY_ID"),
		serverCfg.AuthBootstrapAccessKey,
	))
	o.SecretKey = strings.TrimSpace(firstNonEmpty(
		o.SecretKey,
		os.Getenv("FS_ROOT_PASSWORD"),
		os.Getenv("FSCLI_SECRET_KEY"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		serverCfg.AuthBootstrapSecretKey,
	))

	if o.Timeout <= 0 {
		o.Timeout = 15 * time.Second
	}

	if o.Endpoint == "" {
		return errors.New("admin endpoint is required")
	}
	parsed, err := url.Parse(o.Endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("invalid endpoint %q", o.Endpoint)
	}
	if o.Region == "" {
		return errors.New("region is required")
	}
	if requireCredentials && (o.AccessKey == "" || o.SecretKey == "") {
		return errors.New("credentials required: set --access-key/--secret-key or FSCLI_ACCESS_KEY/FSCLI_SECRET_KEY")
	}
	return nil
}

func endpointFromServerConfig(address string, port int) string {
	host := strings.TrimSpace(address)
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "localhost"
	}
	if port <= 0 || port > 65535 {
		port = 3000
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(port))
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
