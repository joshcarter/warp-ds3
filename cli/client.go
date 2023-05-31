/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/SpectraLogic/ds3_go_sdk/ds3"
	"github.com/SpectraLogic/ds3_go_sdk/ds3/networking"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/joshcarter/warp-ds3/pkg"
	"github.com/minio/cli"
	"github.com/minio/madmin-go/v2"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/certs"
	"github.com/minio/pkg/ellipses"
	"golang.org/x/net/http2"
)

type hostSelectType string

const (
	hostSelectTypeRoundrobin hostSelectType = "roundrobin"
	hostSelectTypeWeighed    hostSelectType = "weighed"
)

func newClient(ctx *cli.Context) func() (cl *ds3.Client, done func()) {
	hosts := parseHosts(ctx.String("host"), ctx.Bool("resolve-host"))
	if len(hosts) == 0 {
		fatalIf(probe.NewError(errors.New("no host defined")), "Unable to create DS3 client")
	}
	if len(hosts) > 1 {
		fatalIf(probe.NewError(errors.New("DS3 only supports one host")), "DS3 only supports one host")
	}

	cl, err := getClient(ctx, hosts[0])
	fatalIf(probe.NewError(err), "Unable to create DS3 client")

	return func() (*ds3.Client, func()) {
		return cl, func() {}
	}
}

// getClient creates a client with the specified host and the options set in the context.
func getClient(ctx *cli.Context, host string) (*ds3.Client, error) {
	cl := ds3.NewClientBuilder(
		&url.URL{Scheme: "http", Host: host},
		&networking.Credentials{
			AccessId: ctx.String("access-key"),
			Key:      ctx.String("secret-key")}).
		BuildClient()

	return cl, nil
}

func clientTransport(ctx *cli.Context) http.RoundTripper {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   ctx.Int("concurrent"),
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
		DisableKeepAlives:  ctx.Bool("disable-http-keepalive"),
	}
	if ctx.Bool("tls") {
		// Keep TLS config.
		tlsConfig := &tls.Config{
			RootCAs: mustGetSystemCertPool(),
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		}
		if ctx.Bool("insecure") {
			tlsConfig.InsecureSkipVerify = true
		}
		tr.TLSClientConfig = tlsConfig

		// Because we create a custom TLSClientConfig, we have to opt-in to HTTP/2.
		// See https://github.com/golang/go/issues/14275
		http2.ConfigureTransport(tr)
	}
	return tr
}

// parseHosts will parse the host parameter given.
func parseHosts(h string, resolveDNS bool) []string {
	hosts := strings.Split(h, ",")
	var dst []string
	for _, host := range hosts {
		if !ellipses.HasEllipses(host) {
			dst = append(dst, host)
			continue
		}
		patterns, perr := ellipses.FindEllipsesPatterns(host)
		if perr != nil {
			fatalIf(probe.NewError(perr), "Unable to parse host parameter")

			log.Fatal(perr.Error())
		}
		for _, lbls := range patterns.Expand() {
			dst = append(dst, strings.Join(lbls, ""))
		}
	}

	if !resolveDNS {
		return dst
	}

	var resolved []string
	for _, hostport := range dst {
		host, port, _ := net.SplitHostPort(hostport)
		if host == "" {
			host = hostport
		}
		ips, err := net.LookupIP(host)
		if err != nil {
			fatalIf(probe.NewError(err), "Could not get IPs for "+hostport)
			log.Fatal(err.Error())
		}
		for _, ip := range ips {
			if port == "" {
				resolved = append(resolved, ip.String())
			} else {
				resolved = append(resolved, ip.String()+":"+port)
			}
		}
	}
	return resolved
}

// mustGetSystemCertPool - return system CAs or empty pool in case of error (or windows)
func mustGetSystemCertPool() *x509.CertPool {
	rootCAs, err := certs.GetRootCAs("")
	if err != nil {
		rootCAs, err = x509.SystemCertPool()
		if err != nil {
			return x509.NewCertPool()
		}
	}
	return rootCAs
}

func newAdminClient(ctx *cli.Context) *madmin.AdminClient {
	hosts := parseHosts(ctx.String("host"), ctx.Bool("resolve-host"))
	if len(hosts) == 0 {
		fatalIf(probe.NewError(errors.New("no host defined")), "Unable to create MinIO admin client")
	}
	cl, err := madmin.New(hosts[0], ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
	fatalIf(probe.NewError(err), "Unable to create MinIO admin client")
	cl.SetCustomTransport(clientTransport(ctx))
	cl.SetAppInfo(appName, pkg.Version)
	return cl
}
