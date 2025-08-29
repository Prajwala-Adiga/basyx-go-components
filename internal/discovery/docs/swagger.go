package docs

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/config"
)

func GetOpenAPISpec(w http.ResponseWriter, r *http.Request, cfg *config.Config, contextPath string) {
	w.Header().Set("Content-Type", "application/yaml")

	// Get the raw spec
	spec := string(OpenAPISpec)

	// Get host info
	host := cfg.Server.Host
	if host == "0.0.0.0" {
		// Use requested host for better usability when 0.0.0.0 is specified
		host = r.Host
	} else if !strings.Contains(host, ":") && cfg.Server.Port != "80" && cfg.Server.Port != "443" {
		// Add port to host if not already included and not using default ports
		host = fmt.Sprintf("%s:%s", host, cfg.Server.Port)
	}

	// Determine protocol (http/https)
	protocol := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		protocol = "https"
	}

	// Create full URL
	serverURL := fmt.Sprintf("%s://%s%s", protocol, host, contextPath)

	// Insert servers section after info and before paths
	infoEndIndex := strings.Index(spec, "\npaths:")
	if infoEndIndex > 0 {
		newServers := fmt.Sprintf("\nservers:\n- url: %s\n  description: Generated server url\n",
			serverURL)
		spec = spec[:infoEndIndex] + newServers + spec[infoEndIndex:]
	}

	w.Write([]byte(spec))
}
