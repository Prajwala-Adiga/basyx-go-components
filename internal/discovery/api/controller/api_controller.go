package api

import (
	"net/http"
	"path"

	factory "github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/api/factory"
	"github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/config"
	discoverydocs "github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/docs"
	openapi "github.com/eclipse-basyx/basyx-go-sdk/pkg/discoveryapi/go"
	"github.com/go-chi/chi/v5"
	httpSwagger "github.com/swaggo/http-swagger"
)

// Register the endpoints for the discovery service
func RegisterEndpoints(r *chi.Mux, contextPath string, cfg *config.Config) func() {
	// Add health endpoint
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	docsBasePath := contextPath
	openapiPath := path.Join(docsBasePath, "/docs/openapi.yaml")
	swaggerUIPath := path.Join(docsBasePath, "/swagger-ui") + "/index.html"

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, swaggerUIPath, http.StatusFound)
	})

	// Serve OpenAPI spec
	r.Get("/docs/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		discoverydocs.GetOpenAPISpec(w, r, cfg, contextPath)
	})

	// Serve Swagger UI
	r.Get("/swagger-ui/*", httpSwagger.Handler(
		httpSwagger.URL(openapiPath), // Use the correct path including context path
		httpSwagger.DeepLinking(true),
		httpSwagger.DocExpansion("list"),
	))

	// Serve the discovery api endpoints
	_, discCtrl, closeFunc := factory.NewApiFactory(cfg).Create()
	for _, rt := range discCtrl.Routes() {
		r.Method(rt.Method, rt.Pattern, rt.HandlerFunc)
	}

	// Server the description endpoint
	descSvc := openapi.NewDescriptionAPIAPIService()
	descCtrl := openapi.NewDescriptionAPIAPIController(descSvc)
	for _, rt := range descCtrl.Routes() {
		r.Method(rt.Method, rt.Pattern, rt.HandlerFunc)
	}

	return closeFunc
}
